from io import BytesIO

import pytest
from starlette.datastructures import FormData, Headers, UploadFile

from docling_proxy.models import cleanup_file_payloads
from docling_proxy.parsing import parse_multipart_form


@pytest.mark.asyncio
async def test_parse_multipart_form_spools_uploads_to_temp_files():
    form = FormData(
        [
            (
                "files",
                UploadFile(
                    file=BytesIO(b"%PDF-1.4\nproxy-test"),
                    filename="alpha.pdf",
                    headers=Headers({"content-type": "application/pdf"}),
                ),
            ),
            ("to_formats", "md"),
        ]
    )

    files, data, proxy_options = await parse_multipart_form(form)
    try:
        assert len(files) == 1
        assert files[0].content is None
        assert files[0].temp_path is not None
        assert files[0].temp_path.exists()
        assert await files[0].read_content() == b"%PDF-1.4\nproxy-test"
        assert data == {"to_formats": "md"}
        assert proxy_options is None
    finally:
        await cleanup_file_payloads(files)
