# Переменные окружения и флаги

Этот документ описывает переменные окружения и флаги для osctl.

## Порядок приоритета

Конфигурация загружается в следующем порядке:
1. **Флаги командной строки**
2. **Переменные окружения**
3. **Общий конфиг файл** (`config.yaml`)
4. **Файл конфигурации индексов** (`osctlindicesconfig.yaml`) - для команд snapshot, indicesdelete, snapshotsdelete, snapshotchecker
5. **Значения по умолчанию** (наименьший приоритет)

## Общие флаги

Эти флаги доступны для всех команд:

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--action` | `OSCTL_ACTION` | Автоматически выполнить команду (snapshot, retention, dereplicator, etc.) | (пусто) |
| `--config` | `OSCTL_CONFIG` | Путь к файлу конфигурации | `config.yaml` |
| `--os-url` | `OPENSEARCH_URL` | URL OpenSearch | `https://opendistro:9200` |
| `--os-recoverer-url` | `OPENSEARCH_RECOVERER_URL` | URL OpenSearch Recoverer | `https://opendistro-recoverer:9200` |
| `--cert-file` | `OPENSEARCH_CERT_FILE` | Путь к сертификату | `/etc/ssl/certs/admin-crt.pem` |
| `--key-file` | `OPENSEARCH_KEY_FILE` | Путь к приватному ключу | `/etc/ssl/certs/admin-key.pem` |
| `--ca-file` | `OPENSEARCH_CA_FILE` | Путь к CA | (пусто) |
| `--timeout` | `OPENSEARCH_TIMEOUT` | Таймаут запросов | `300s` |
| `--retry-attempts` | `OPENSEARCH_RETRY_ATTEMPTS` | Количество повторных попыток для запросов в апи | `3` |
| `--date-format` | `OPENSEARCH_DATE_FORMAT` | Формат даты в названиях индексов и снапшотов | `%Y.%m.%d` |
| `--recoverer-date-format` | `RECOVERER_DATE_FORMAT` | Формат даты для индексов у Recoverer | `%d-%m-%Y` |
| `--madison-url` | `MADISON_URL` | URL API Madison | `https://madison.flant.com/api/events/custom/` |
| `--madison-key` | `MADISON_KEY` | Ключ API Madison | (пусто) |
| `--osd-url` | `OPENSEARCH_DASHBOARDS_URL` | URL OpenSearch Dashboards | (пусто) |
| `--osctl-indices-config` | `OSCTL_INDICES_CONFIG` | Путь к конфигу индексов - для snapshot, indicesdelete, snapshotsdelete, snapshotchecker | `osctlindicesconfig.yaml` |
| `--dry-run` | `DRY_RUN` | Показать что будет сделано без выполнения | `false` |
| `--snap-repo` | `SNAPSHOT_REPOSITORY` | Название репо для снапшотов | (пусто) |

## Параметр action

Параметр `action` позволяет автоматически выполнить нужную команду без указания её в командной строке. Сделано для деплойментов и кронджоб в Kubernetes.

### Доступные значения action:
- `snapshots` 
- `snapshot-manual` 
- `snapshotsdelete` 
- `snapshotschecker` 
- `indicesdelete` 
- `retention` 
- `dereplicator`
- `coldstorage` 
- `extracteddelete`
- `danglingchecker`
 - `sharding`
 - `indexpatterns`
 - `datasource`

### Примеры использования:

**В config.yaml:**
```yaml
action: "snapshot"
opensearch_url: "https://opendistro:9200"
# ...
```

**Через переменную окружения:**
```bash
export OSCTL_ACTION=retention
osctl
```

**Через флаг:**
```bash
osctl --action=snapshot
```

## Флаги команд

### `coldstorage`

Перемещает индексы на узлы cold, если они старше N дней.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--cold-attribute` | `COLD_ATTRIBUTE` | Атрибут узлов для cold | (пусто) |
| `--hot-count` | `HOT_COUNT` | Количество дней, которые нужно держать индексы в hot | `3` |

**Ключи в конфиг файле:**
- `cold_attribute`
- `hot_count`

### `retention`

Удаляет старые индексы при превышении порога использования диска.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--retention-threshold` | `RETENTION_THRESHOLD` | Порог использования диска в процентах | `75` |

**Ключи в конфиг файле:**
- `retention-threshold`

### `dereplicator`

Уменьшает количество реплик до 0 для индексов старше указанного количества дней.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--dereplicator-days-count` | `DEREPLICATOR_DAYS` | Какое количество дней держать индексы с репликами | `2` |
| `--dereplicator-use-snapshot` | `DEREPLICATOR_USE_SNAPSHOT` | Проверять снапшоты перед уменьшением реплик | `false` |

**Ключи в конфиг файле:**
- `dereplicator_days_count`
- `dereplicator_use_snapshot`

### `extracteddelete`

Удаляет extracted индексы, которые больше не нужны.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--days` | `EXTRACTED_DAYS` | Какое количество дней держать extracted индексы | `7` |
| `--extracted-pattern` | `EXTRACTED_PATTERN` | Префикс для extracted индексов | `extracted_` |
| `--opensearch_recoverer_url` | `OPENSEARCH_RECOVERER_URL` | Url рековерера | `https://opendistro-recoverer:9200` |
| `--recoverer_date_format` | `RECOVERER_DATE_FORMAT` | Формат даты у extracted индексов | `%d-%m-%Y` |

**Ключи в конфиг файле:**
- `opensearch_recoverer_url`
- `extracted_pattern`
- `extracted_days`
- `recoverer_date_format`

### `snapshots`, `snapshot-manual`

Создает снапшоты индексов.

**Специальные флаги для snapshot-manual:**
| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--snapshot-manual-kind` | `SNAPSHOT_KIND` | Тип паттерна: prefix или regex | `prefix` |
| `--snapshot-manual-value` | `SNAPSHOT_VALUE` | Значение паттерна | (пусто) |
| `--snapshot-manual-name` | `SNAPSHOT_NAME` | Имя снапшота (обязательно для regex) | (пусто) |
| `--snapshot-manual-system` | `SNAPSHOT_SYSTEM` | Флаг системного индекса (получает индексы с точкой, независимо от даты) | `false` |

### `sharding`

Рассчитывает шардирование и создает/обновляет index template.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--sharding-target-size-gib` | `SHARDING_TARGET_SIZE_GIB` | Целевой размер шарда (GiB, максимум 50) | `25` |
| `--exclude-sharding` | `EXCLUDE_SHARDING` | Регекс для исключения паттернов | (пусто) |

**Ключи в конфиг файле:**
- `sharding_target_size_gib`
- `exclude_sharding`

### `indexpatterns`

Управляет index patterns в Kibana через OpenSearch API (индекс `.kibana`)

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--kibana-index-regex` | `KIBANA_INDEX_REGEX` | Регекс для построения паттернов | `^([\\w-]+)-([\\w-]*)(\\d{4}[\\.-]\\d{2}[\\.-]\\d{2}(?:[\\.-]\\d{2})*)$` |
| `--kibana-multitenancy` | `KIBANA_MULTITENANCY` | Режим multitenancy | `false` |
| `--kibana-tenants-config` | `KIBANA_TENANTS_CONFIG` | Путь к YAML с тенантами и index patterns | `osctltenants.yaml` |
| `--recoverer-enabled` | `RECOVERER_ENABLED` | Создавать `extracted_*` с ссылкой на data-source | `false` |

Примечание: Список тенантов всегда берётся из `--kibana-tenants-config`.

**Ключи в конфиг файле:**
- `kibana_index_regex`
- `kibana_multitenancy`
- `recoverer_enabled`

### `datasource`

Создает Kibana data-source в нужных тенантах и при multidomain синхронизирует секреты.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--kibana-user` | `KIBANA_API_USER` | Пользователь API Kibana | (пусто) |
| `--kibana-pass` | `KIBANA_API_PASS` | Пароль API Kibana | (пусто) |
| `--datasource-name` | `DATA_SOURCE_NAME` | Название data-source | `recoverer` |
| `--kube-namespace` | `KUBE_NAMESPACE` | Namespace для секретов | `infra-elklogs` |
| `--kibana-multidomain-enabled` | `KIBANA_MULTIDOMAIN_ENABLED` | Управление секретом `multi-certs` и перезапуск Kibana если было обновление сертификатов | `false` |
| `--remote-crt` | `REMOTE_CRT` | Конкатенированные base64 сертификаты, разделённые `|` (используется при multidomain) | (пусто) |

В режиме multitenancy список тенантов берется из `--kibana-tenants-config` (`KIBANA_TENANTS_CONFIG`), файл обязателен.

Переменные окружения и ключи конфига для multidomain:
- `REMOTE_CRT` / `remote_crt`: конкатенированные base64 сертификаты, разделённые `|` (будут склеены и объединены с `recoverer-certs/ca.crt`, если есть)

