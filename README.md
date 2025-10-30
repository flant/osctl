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

## Конфигурация

### Единая конфигурация (`config.yaml`)

Пример в `config.yaml`

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
