# osctl - Инструмент управления жизненным циклом индексов и снапшотов OpenSearch

По-умолчанию предполагается использование `config.yaml` в котором описаны параметры запуска. Для автоматического выполнения команд можно использовать параметр `action`.

Все, что указано в конфгиах можно переопределять через флаги, а также большинство команд поддерживают `--dry-run`.

## Быстрый старт

### Использование с параметром action (рекомендуется)
```bash
# Создать config.yaml с нужной командой
cat > config.yaml << EOF
action: "snapshots"
opensearch_url: "https://your-opensearch:9200"
cert_file: "/path/to/cert.pem"
key_file: "/path/to/key.pem"
ca_file: "/path/to/ca.pem"
EOF

# Запустить osctl - команда выполнится автоматически
osctl
osctl --dry-run
```

### Вариант использования через команды и флаги
```bash
osctl snapshots --config=custom_config.yaml --osctl-indices-config=custom_osctl_indices_config.yaml
```

## Доступные команды (action)

| Команда | Назначение |
|---------|------------|
| `snapshots` | Создание снапшотов согласно конфигурации |
| `snapshotsdelete` | Удаление снапшотов согласно конфигурации |
| `indicesdelete` | Удаление индексов согласно конфигурации |
| `snapshotschecker` | Нахождение отсутствующих снапшотов |
| `snapshot-manual` | Создание снапшотов по флагам |
| `retention` | Удаление индексов со снапшотами при превышении некоторого порога  |
| `dereplicator` | Уменьшение числа реплик |
| `coldstorage` | Миграция в холодное хранилище |
| `extracteddelete` | Удаление extracted индексов |
| `danglingchecker` | Проверка dangling индексов |
| `sharding` | Автоматическое выставление оптимального числа шардов |
| `indexpatterns` | Управление index patterns в Kibana |
| `datasource` | Создание Kibana data-source |

## Конфигурация

### Единая конфигурация (`config.yaml`)

Пример полного `config.yaml` (все ключи опциональны, если не указано иное):
```yaml
# Автозапуск команды (см. список команд выше)
action: "snapshots"

# Подключение к OpenSearch
opensearch_url: "https://your-opensearch:9200"
cert_file: "/path/to/cert.pem"
key_file: "/path/to/key.pem"
ca_file: "/path/to/ca.pem"
timeout: "300s"
retry_attempts: 3

# Форматы дат
date_format: "%Y.%m.%d"
recoverer_date_format: "%d-%m-%Y"

# Madison (опционально)
madison_url: "https://madison.flant.com/api/events/custom/"
madison_key: ""
madison_project: "lm-elk"

# Общие опции
dry_run: false
snapshot_repo: "s3-backup"

# Dereplicator
dereplicator_days_count: 2
dereplicator_use_snapshot: false

# Retention
retention_threshold: 75

# Coldstorage
hot_count: 4
cold_attribute: "cold"

# Extracted delete
extracted_pattern: "extracted_"
extracted_days: 7
opensearch_recoverer_url: "https://opendistro-recoverer:9200"

# Sharding
sharding_target_size_gib: 25
exclude_sharding: ""

# Index patterns / Multitenancy
kibana_index_regex: "^([\\w-]+)-([\\w-]*)(\\d{4}[\\.-]\\d{2}[\\.-]\\d{2}(?:[\\.-]\\d{2})*)$"
kibana_multitenancy: false
kibana_tenants_config: "osctltenants.yaml"
recoverer_enabled: false

# Datasource / Kibana
osd_url: "https://dashboards.example.com"
kibana_user: ""
kibana_pass: ""
datasource_name: "recoverer"
kube_namespace: "infra-elklogs"
kibana_multidomain_enabled: false
remote_crt: ""  # base64 сертификаты, разделённые | (для multidomain)
```

### Конфигурация индексов (`osctlindicesconfig.yaml`)

Пример в `config-example/osctlindicesconfig.yaml`

## Приоритет конфигурации

1. **Флаги командной строки** (наивысший приоритет)
2. **Переменные окружения** 
3. **Единый конфиг** (`config.yaml`)
4. **Значения по умолчанию** (наименьший приоритет)

## 📚 Документация

- **[ENVIRONMENT_VARIABLES_AND_FLAGS.md](ENVIRONMENT_VARIABLES_AND_FLAGS.md)** - Полный справочник по всем флагам и переменным окружения
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Подробные алгоритмы и архитектура системы
- **Встроенная справка**: `osctl [команда] --help`
