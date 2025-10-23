## Архитектура

### Структура проекта

```
osctl/
├── cmd/
│   └── main.go                    # Точка входа приложения
├── internal/
│   ├── commands/                 # Команды CLI
│   │   ├── root.go              # Общие флаги и список команд
│   │   ├── snapshot.go          # Создание снапшотов
│   │   ├── indicesdelete.go     # Удаление индексов согласно values ( с проверкой из либы snapshotchecker, с генерацией алерта  - не могу удалить индекс, с флагом нужно ли это вообще)
|   |   ├── snapshotdelete.go    # Удаление снапшотов согласно values 
│   │   ├── retention.go         # Удаление самых старых и больших индексов
│   │   ├── sharding.go          # Автоматическое шардирование
│   │   ├── indexpatterns.go     # Kibana index patterns
│   │   ├── datasource.go        # Kibana data sources
│   │   ├── dereplicator.go      # Уменьшение реплик до 0
│   │   ├── snapshotchecker.go   # Проверка snapshots
│   │   ├── danglingchecker.go   # Проверка dangling индексов
│   │   ├── coldstorage.go       # Миграция в cold storage
│   │   └── extracteddelete.go   # Удаление extracted индексов - это не нужное 
│   ├── opensearch/              # OpenSearch API клиент - net/http запросы
│   │   └── client.go
│   ├── kibana/                  # Kibana API клиент
│   │   └── client.go
│   ├── madison/                 # Madison алерты
│   │   └── client.go
│   └── logging/                 # Логирование
│       └── logger.go
│   └── utils/                 # Формат даты в go формат
│       └── date.go
├── Dockerfile
├── go.mod
├── go.sum
└── README.md
```

### Компоненты

#### 1. CLI Commands (internal/commands/)

Каждая команда представляет собой отдельный модуль с:
- Собственными флагами
- Валидацией параметров
- Логикой

#### 2. OpenSearch Client (internal/opensearch/)

Клиент для работы с OpenSearch API:
- Аутентификация
- HTTP клиент с retry логикой
- Методы для всех операций с индексами
- Управление снапшотами
- Настройки индексов

#### 3. Kibana Client (internal/kibana/)

Клиент для работы с Kibana API:
- Создание index patterns
- Управление data sources
- Multitenancy

#### 4. Madison Client (internal/madison/)

Клиент для отправки алертов:
- Создание алертов
- Отправка в Madison API

#### 5. Logging (internal/logging/)

Структурированное логирование:
- JSON формат
- Уровни логирования
- Контекстная информация

## Алгоритмы для каждого флага

### 1. **snapshot** - Создание снапшотов индексов

**Алгоритм:**
1. **Проверка параметров**: `index_name`, `system_index`, `snap_repo`, `check-indices-exists`, `wildcard`
2. **Создаем снапшот**:
   - Проверяем существование индекса за вчера через `GET /_cat/indices/{index}-{yesterday}*` - если нет - exit 0, но если есть флаг - check-indices-exists - exit 1
   - Ждём завершения активных snapshot'ов через `GET /_snapshot/_status`
   - Ждём завершения тасков через `GET /_tasks`
   - Проверяем существование snapshot'а через `GET /_snapshot/{snap_repo}/{index}-{today}` - если он партиал или его нет - продолжаем (партиал очищаем)
   - Создаём snapshot через `PUT /_snapshot/{snap_repo}/{index}-{today}` с телом:
     ```json
     {
       "indices": "{index}-{yesterday}*",
       "ignore_unavailable": true,
       "include_global_state": false
     }
     ```
   - При ошибке: удаляем неудачный snapshot и фейлим - рестарт будет от самой кронджобы
   - **Джобу не генерим хельмом если в values snapshot_enable false**

3. **indicesdelete Удаление снапшотов**: делаем по алогритму
   - **Проверка параметров**: `dangle_snapshots`
   - Фильтрация по префиксу: snapshots с префиксом или regex в зависимости от значени kind `{index_name}`
   - Фильтрация по возрасту: старше N дней по дате создания на основе имени
   - Удаление: через `DELETE /_snapshot/{snap_repo}/{snapshot_name}` для каждого подходящего snapshot
   - Если есть флаг dangle_snapshots - то проверяются ВСЕ снапшоты - которые не попадают в curator.yaml и они удаялеются по флагу s3_snapshots.unit_count.all 

4. **snapshotdelete Удаление индексов**: делаем по алогритму

   - Фильтрация по префиксу: `{index_name}`
   - Фильтрация по возрасту: старше N дней по дате создания на основе имени
   - Удаление: через `DELETE /index_name` для каждого подходящего индекса
   - В случае unknown - фильтрация по всем идексам, которых нет в values кроме restored-|extracted_ и системных

Если есть флаг wildcard=true то используем * после префикса ( будет 1 снапшот для индекосов аля production-blabla-дата production-herher-дата  )

**Все что с  . не удаляем**

**Флаги:**
```bash
# Снапшот конкретного индекса
osctl snapshot --index=infra --snap-repo=s3-backup

# Снапшот unknown индексов
osctl snapshot --unknown --snap-repo=s3-backup

# Только удаление
osctl snapshot-delete --index=infra --snap-repo=s3-backup
```

### 2. **retention** - Удаление самых старых и больших индексов

**Алгоритм:**
1. **Расчёт утилизации**: Получаем `GET /_cat/allocation`, считаем среднюю утилизацию
2. **Если утилизация > порога**:
   - Получаем кандидатов через `GET /_cat/indices` (исключая сегодня, вчера, `extracted_*`)
   - Сортируем по размеру (убывание)
   - Для каждого индекса:
     - Проверяем snapshots в репо через `GET /_snapshot/{snap_repo}/*-{tomorrow}`
     - Если есть - удаляем индекс через `DELETE /{index}`
     - Повторяем пока утилизация не упадёт ниже порога

- **Порог утилизации**: по умолчанию 75%
- **Исключения**: индексы за сегодня, вчера, `extracted_*`

**Флаги:**
```bash
osctl retention --threshold=75 --snap-repo=s3-backup --endpoint=opendistro
```

### 3. **sharding** - Автоматическое шардирование

**Алгоритм:**

1. **Получение всех индексов**: `GET /_cat/indices?bytes=b&s=ss` - делается чтобы учитывать не размер сегодняешнего а максимальный размер вообще
2. **Получение индексов за сегодня**: `GET /_cat/indices/-.*,*-{today}*?bytes=b&s=ss`
3. **Для каждого индекса за сегодня**:
   - Генерируем pattern: `{index_name_without_date}*`
   - Ищем максимальный размер среди индексов с pattern `{index_name_without_date}` - если больше указанного размера - это триггер для шардинга
   - Вычисляем приоритет: `количество_дефисов * 1000` (infra- отличается от infra-elklogs- приоритетами)
   - Создаём template: `{index_name_without_date}-sharding`
4. **Расчёт shards**: `shards_needed = max_size / 25GiB + 1`, но с ограничением по числу нод
5. **Создание template** с рассчитанным `priority` и настройками:
   - `number_of_shards`: рассчитанное количество
   - `number_of_replicas`: 1
   - `mapping.total_fields.limit`: 2000
   - `routing.allocation.require.temp`: "hot" (если включен cold cluster)
   - `query.default_field`: ["message","text","log","original_message"]
6. Если шаблон уже есть - **меняем ТОЛЬКО number_of_shards**

- 25GiB задаем параметром, с проверкой что он не больше 50Gib
- nodes-count = datanodes ( берем из кластера! - все ноды с типом data, не мастер и не колд)
- индексы с . не трогаем

**Флаги:**
```bash
# Обычные индексы
osctl sharding --shard-size=26843545600

# Часовые индексы  
osctl sharding --shard-size=26843545600
```

### 4. **indexpatterns** - Управление Kibana index patterns

**Алгоритм (multitenancy):**
1. **Для каждого тенанта**:
   - Проверяем `.kibana*_tenant` через `GET /_cat/aliases/.kibana*_{tenant}`
   - Получаем существующие patterns через `GET /{tenant_index}/_search?q=type:index-pattern`
   - Создаём недостающие через `POST /{tenant_index}/_doc/index-pattern:{uuid}`

**Алгоритм (без multitenancy):**
   - Построение паттернов из сегодняшних индексов по регулярке
   - Сравнение с существующими в `.kibana`
   - Создание недостающих паттернов
   - Дополнительно создаём `extracted_*` с reference на data-source

- extracted_* по соображением безопасности не создается в режиме multitenancy
- regex нужен только в режиме Без multitenancy для исключения дерьма

**Флаги:**
```bash
# Без multitenancy
osctl indexpatterns --multitenancy=false --regex="^(.*?)-\d{4}\.\d{2}\.\d{2}.*$" --osd-url=https://kibana.example.com

# С multitenancy
osctl indexpatterns --multitenancy=true --tenants="tenant1,tenant2" --osd-url=https://kibana.example.com

# С recoverer
osctl indexpatterns --multitenancy=false --regex="^(.*?)-\d{4}\.\d{2}\.\d{2}.*$" --recoverer-enabled=true --osd-url=https://kibana.example.com
```

### 5. **datasource** - Создание Kibana data sources

**Алгоритм:**
1. **Проверка существования**: через `GET /api/saved_objects/_find?type=data-source`
2. **Создание**: через `POST /api/saved_objects/data-source` с basic auth
3. **Multidomain режим**:
   - Собираем CA из env и kube секретов
   - Сравниваем с `multi-certs` секретом из REMOTE_CRT
   - При изменении: обновляем секрет и рестартуем kibana

**Флаги:**
```bash
osctl datasource --title=recoverer --os-url=https://opendistro-recoverer:9200 --osd-url=https://kibana.example.com

# С multidomain
osctl datasource --title=recoverer --os-url=https://opendistro-recoverer:9200 --osd-url=https://kibana.example.com --multidomain=true --namespace=default
```

### 6. **dereplicator** - Уменьшение реплик старых индексов

**Алгоритм:**
1. **Фильтрация по возрасту**: индексы старше N дней 
2. **Исключения**:
   - Индексы за последние N дней (исключаются)
   - Системные индексы (исключаются)
   - Индексы с 0 репликами (исключаются)
3. **Проверка snapshots**:
   - Получаем snapshots через `GET /_snapshot/{snap_repo}/*?verbose=false`
   - Проверяем SUCCESS статус для каждого индекса
4. **Установка 0 реплик**: через `PUT /{index}/_settings` с телом:
   ```json
   {
     "index": {
       "number_of_replicas": 0
     }
   }
   ```

**Флаги:**
```bash

osctl dereplicator --days-count=2 --use-snapshot=true --snap-repo=s3-backup --os-url=https://opendistro:9200
```

### 7. **snapshotchecker** - Проверка наличия snapshots

**Алгоритм:**
1. **Получение индексов**: за позавчерашний день
2. **Фильтрация**: по whitelist/exclude list
3. **Проверка snapshots**: за вчерашний день в репозитории
4. **Алерт в Madison**: если найдены индексы без snapshots

**Флаги:**
```bash
# Whitelist режим
osctl snapshotchecker --snap-repo=s3-backup --whitelist="nginx,osctl" --madison-key=xxx --madison-project=yyy --osd-url=https://kibana.example.com (для алертинга только?)

# Exclude режим
osctl snapshotchecker --snap-repo=s3-backup --exclude-list="system" --madison-key=xxx --madison-project=yyy --osd-url=https://kibana.example.com 

# Без Madison
osctl snapshotchecker --snap-repo=s3-backup --whitelist="nginx,osctl"
```

### 8. **danglingchecker** - Проверка dangling индексов

**Алгоритм:**
1. **Запрос dangling**: `GET /_dangling?pretty`
2. **Если найдены**: отправляем алерт в Madison

**Флаги:**
```bash
osctl danglingchecker --os-url=https://opendistro:9200 --osd-url=https://kibana.example.com --madison-key=xxx --madison-project=yyy

```

### 9. **coldstorage** - Миграция в cold storage

**Алгоритм:**
1. **Фильтрация по возрасту**: индексы старше `coldCluster.hotCount` дней по формату `%Y.%m.%d`
2. **Перемещение в cold**: через `PUT /{index}/_settings` с allocation settings:
   ```json
   {
     "index": {
       "routing.allocation.require.temp": "cold",
       "number_of_replicas": 0
     }
   }
   ```


**Флаги:**
```bash
osctl coldstorage --index-pattern="*" --set-replicas-zero=true --move-to-cold=true --os-url=https://opendistro:9200
```

### 10. **extracteddelete** - Удаление extracted индексов

**Алгоритм:**
1. **Фильтрация по префиксу**: индексы с префиксом `extracted`
2. **Фильтрация по возрасту**: старше 2 дней по формату `%d-%m-%Y` (например, `extracted_15-12-2024`)
3. **Удаление**: через `DELETE /{index}` для каждого подходящего индекса

**Флаги:**
```bash
# Обычные сертификаты
osctl extracteddelete --selector="extracted_*" --os-url=https://opendistro:9200

# С recoverer сертификатами
osctl extracteddelete --selector="extracted_*" --os-url=https://opendistro-recoverer:9200 --use-recoverer-certs=true
```

## Общие флаги для всех команд

```bash
# TLS сертификаты
--cert-file=/etc/ssl/certs/admin-crt.pem
--key-file=/etc/ssl/certs/admin-key.pem
--ca-file=/etc/ssl/certs/elk-root-ca.pem

# OpenSearch endpoint
--os-url=https://opendistro:9200

# Таймауты
--timeout=30s
--retry-attempts=3

# Формат даты в имени индекса и снапшота
--date-format="%Y.%m.%d"

# Алертинг
--madison-url=https://madison.flant.com/api/events/custom/
--osd-url=""
--madison-key=""
--madison-project=""
```
