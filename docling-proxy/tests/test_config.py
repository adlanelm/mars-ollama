from docling_proxy.config import Settings


def test_settings_uses_deprecated_active_child_alias_when_needed(monkeypatch):
    monkeypatch.delenv("DOCLING_PROXY_ACTIVE_CHILD_LIMIT", raising=False)

    settings = Settings(max_concurrency=3)

    assert settings.active_child_limit == 3
    assert settings.work_concurrency == 3


def test_settings_prefers_new_names_over_deprecated_values(monkeypatch):
    monkeypatch.setenv("DOCLING_PROXY_ACTIVE_CHILD_LIMIT", "4")

    settings = Settings(active_child_limit=2, max_concurrency=7, work_concurrency=5)

    assert settings.active_child_limit == 2
    assert settings.work_concurrency == 5


def test_deprecated_environment_messages_report_old_names(monkeypatch):
    monkeypatch.setenv("DOCLING_PROXY_MAX_CONCURRENCY", "2")
    monkeypatch.setenv("DOCLING_PROXY_LOCAL_DOCLING_WORKERS", "4")

    settings = Settings()
    messages = settings.deprecated_environment_messages()

    assert any("DOCLING_PROXY_MAX_CONCURRENCY is deprecated" in message for message in messages)
    assert any("DOCLING_PROXY_LOCAL_DOCLING_WORKERS is deprecated and ignored" in message for message in messages)
