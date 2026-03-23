import json
from typing import Optional, Dict, Any, List, Tuple

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from mappers import fsp_to_hope, hope_to_fsp
from mappers import hstp_fsp_export, hstp_verification
from mappers import hstp_fsp_reconciliation
from mappers import multi_excel_column_merge
from mappers import hstp_signature_list_from_zip
from mappers import sp_signature_list_from_zip



app = FastAPI(title="SSD Recon Mapping API")

# CORS: allow Live Server + localhost variants
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",
        "http://localhost:5500",
        "http://127.0.0.1:8000",
        "http://localhost:8000",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve UI from /ui (optional)
# Open: http://127.0.0.1:8000/ui/index.html
app.mount("/ui", StaticFiles(directory=".", html=True), name="ui")

# ---------------------------
# API: health
# ---------------------------
@app.get("/api")
def api_root():
    return {"message": "API running", "endpoints": ["/headers", "/transform", "/hstp/inspect", "/hstp/generate", "/verification/headers", "/verification/merge"]}


# ---------------------------
# API: Flexible Excel mapping
# ---------------------------
MAPPERS = {
    "fsp_to_hope": fsp_to_hope,
    "hope_to_fsp": hope_to_fsp,
}

def _get_mapper(direction: str):
    direction = (direction or "").strip()
    if direction not in MAPPERS:
        raise HTTPException(status_code=400, detail="Invalid direction. Use: fsp_to_hope or hope_to_fsp.")
    return MAPPERS[direction]


@app.post("/headers")
async def headers(
    file: UploadFile = File(...),
    password: str = Form(""),
    header_row: int = Form(2),
    direction: str = Form(...),
):
    content = await file.read()
    pwd: Optional[str] = password.strip() or None

    mapper = _get_mapper(direction)
    try:
        cols = mapper.get_headers(content, pwd, header_row)
        return {"columns": cols, "header_row": header_row, "direction": direction}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/transform")
async def transform(
    file: UploadFile = File(...),
    password: str = Form(""),
    header_row: int = Form(2),
    direction: str = Form(...),
    mapping_config: str = Form(...),
):
    content = await file.read()
    pwd: Optional[str] = password.strip() or None

    mapper = _get_mapper(direction)

    try:
        cfg: Dict[str, Any] = json.loads(mapping_config)
    except Exception:
        raise HTTPException(status_code=400, detail="mapping_config is not valid JSON.")

    try:
        out_bytes = mapper.transform(content, pwd, header_row, cfg)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    filename = f"{direction}_mapped_output.xlsx"
    return StreamingResponse(
        iter([out_bytes]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# --------------------------------
# API: HSTP -> FSP ZIP export
# --------------------------------
@app.post("/hstp/inspect")
async def hstp_inspect(
    zipfile_upload: UploadFile = File(...),
    header_row: int = Form(2),
    excel_name: str = Form(""),
):
    zbytes = await zipfile_upload.read()

    try:
        excel_files = hstp_fsp_export.list_excel_files_in_zip(zbytes)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read ZIP: {str(e)}")

    if not excel_files:
        raise HTTPException(status_code=400, detail="No Excel files (.xlsx/.xls) found inside the ZIP.")

    excel_name = (excel_name or "").strip()
    if not excel_name:
        return {"excel_files": excel_files}

    try:
        df = hstp_fsp_export.read_excel_from_zip(zbytes, excel_name, header_row)
        headers = [str(c) for c in df.columns.tolist()]
        return {"excel_files": excel_files, "selected_excel": excel_name, "columns": headers, "header_row": header_row}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/hstp/generate")
async def hstp_generate(
    zipfile_upload: UploadFile = File(...),
    excel_name: str = Form(...),
    header_row: int = Form(2),
    fsp: str = Form(""),
    output_name: str = Form("hstp_export"),
    zip_password: str = Form(...),
    mapping_config: str = Form(...),
):
    zbytes = await zipfile_upload.read()

    try:
        cfg: Dict[str, Any] = json.loads(mapping_config)
    except Exception:
        raise HTTPException(status_code=400, detail="mapping_config is not valid JSON.")

    try:
        df = hstp_fsp_export.read_excel_from_zip(zbytes, excel_name, header_row)
        out_df = hstp_fsp_export.build_output_df(df, cfg)

        base_name = output_name
        if fsp.strip():
            base_name = f"{output_name}_{fsp.strip()}"

        zip_bytes = hstp_fsp_export.generate_passworded_zip_with_csv(
            out_df=out_df,
            output_base_name=base_name,
            zip_password=zip_password,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    filename = f"{hstp_fsp_export.safe_basename(output_name)}.zip"
    return StreamingResponse(
        iter([zip_bytes]),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# --------------------------------
# API: HSTP data verification merge
# --------------------------------
@app.post("/verification/headers")
async def verification_headers(
    hope_file: UploadFile = File(...),
    verification_files: List[UploadFile] = File(...),
    hope_header_row: int = Form(2),
    verification_header_row: int = Form(2),
):
    hope_bytes = await hope_file.read()

    ver_list: List[Tuple[bytes, str, int]] = []
    for vf in verification_files:
        vb = await vf.read()
        ver_list.append((vb, vf.filename, verification_header_row))

    try:
        data = hstp_verification.get_headers(
            hope_bytes=hope_bytes,
            hope_filename=hope_file.filename,
            hope_header_row=hope_header_row,
            verification_files=ver_list,
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/verification/merge")
async def verification_merge(
    hope_file: UploadFile = File(...),
    verification_files: List[UploadFile] = File(...),
    hope_header_row: int = Form(2),
    verification_header_row: int = Form(2),
    hope_id_col: str = Form(...),
    verification_id_col: str = Form(...),
    overwrite_existing: str = Form("true"),
    mapping_config: str = Form(...),
):
    hope_bytes = await hope_file.read()

    ver_list: List[Tuple[bytes, str, int]] = []
    for vf in verification_files:
        vb = await vf.read()
        ver_list.append((vb, vf.filename, verification_header_row))

    try:
        cfg = json.loads(mapping_config)
        mapping_rows = cfg.get("mappings", [])
        if not isinstance(mapping_rows, list):
            raise ValueError("mappings must be a list.")

        overwrite = (overwrite_existing or "").strip().lower() == "true"

        out_bytes = hstp_verification.merge(
            hope_bytes=hope_bytes,
            hope_filename=hope_file.filename,
            hope_header_row=hope_header_row,
            verification_files=ver_list,
            hope_id_col=hope_id_col,
            verification_id_col=verification_id_col,
            mapping_rows=mapping_rows,
            overwrite_existing=overwrite,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return StreamingResponse(
        iter([out_bytes]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=hstp_verification_updated_hope.xlsx"},
    )

@app.post("/recon/headers")
async def recon_headers(
    fsp_file: UploadFile = File(...),
    hope_file: UploadFile = File(...),
    fsp_password: str = Form(""),
    fsp_header_row: int = Form(2),
    hope_header_row: int = Form(2),
):
    fsp_bytes = await fsp_file.read()
    hope_bytes = await hope_file.read()
    pwd = fsp_password.strip() or None

    try:
        data = hstp_fsp_reconciliation.get_headers(
            fsp_bytes=fsp_bytes,
            fsp_password=pwd,
            fsp_header_row=fsp_header_row,
            hope_bytes=hope_bytes,
            hope_header_row=hope_header_row,
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/recon/generate")
async def recon_generate(
    fsp_file: UploadFile = File(...),
    hope_file: UploadFile = File(...),
    fsp_password: str = Form(""),
    fsp_header_row: int = Form(2),
    hope_header_row: int = Form(2),
    hope_key: str = Form(...),
    fsp_key: str = Form(...),
    output_name: str = Form("hstp_fsp_reconciliation"),
    mapping_config: str = Form(...),
):
    fsp_bytes = await fsp_file.read()
    hope_bytes = await hope_file.read()
    pwd = fsp_password.strip() or None

    try:
        cfg = json.loads(mapping_config)
        out_bytes = hstp_fsp_reconciliation.build_output(
            fsp_bytes=fsp_bytes,
            fsp_password=pwd,
            fsp_header_row=fsp_header_row,
            hope_bytes=hope_bytes,
            hope_header_row=hope_header_row,
            hope_key=hope_key,
            fsp_key=fsp_key,
            mapping_config=cfg,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    safe_name = (output_name or "hstp_fsp_reconciliation").strip() or "hstp_fsp_reconciliation"
    if not safe_name.lower().endswith(".xlsx"):
        safe_name += ".xlsx"

    return StreamingResponse(
        iter([out_bytes]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={safe_name}"},
    )


@app.post("/multi_merge/headers")
async def multi_merge_headers(
    files: List[UploadFile] = File(...),
    header_row: int = Form(2),
    selected_sheets: str = Form(...),  # JSON list, same order as files
):
    try:
        sheets = json.loads(selected_sheets)
        if not isinstance(sheets, list) or len(sheets) != len(files):
            raise ValueError("selected_sheets must be a JSON list with same length as uploaded files.")

        payload = []
        for i, f in enumerate(files):
            b = await f.read()
            payload.append({"bytes": b, "sheet": sheets[i]})

        cols = multi_excel_column_merge.get_union_headers(payload, header_row=header_row)
        return {"columns": cols}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))



@app.post("/multi_merge/generate")
async def multi_merge_generate(
    files: List[UploadFile] = File(...),
    header_row: int = Form(2),
    output_name: str = Form("merged.xlsx"),
    selected_columns: str = Form(...),
    selected_sheets: str = Form(...),
):
    try:
        cols = json.loads(selected_columns)
        sheets = json.loads(selected_sheets)

        if not isinstance(cols, list) or len(cols) == 0:
            raise ValueError("selected_columns must be a non-empty JSON list.")
        if not isinstance(sheets, list) or len(sheets) != len(files):
            raise ValueError("selected_sheets must be a JSON list with same length as uploaded files.")

        payload = []
        for i, f in enumerate(files):
            b = await f.read()
            payload.append({"name": f.filename, "bytes": b, "sheet": sheets[i]})

        out_bytes = multi_excel_column_merge.build_merged_file(
            files=payload,
            header_row=header_row,
            selected_columns=cols,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    safe_name = (output_name or "merged.xlsx").strip() or "merged.xlsx"
    if not safe_name.lower().endswith(".xlsx"):
        safe_name += ".xlsx"

    return StreamingResponse(
        iter([out_bytes]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={safe_name}"},
    )




@app.post("/multi_merge/sheets")
async def multi_merge_sheets(files: List[UploadFile] = File(...)):
    try:
        result = []
        for f in files:
            b = await f.read()
            sheets = multi_excel_column_merge.list_sheets(b)
            result.append({"filename": f.filename, "sheets": sheets})
        return {"files": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    



@app.post("/signature_list/headers")
async def hstp_signature_list_headers(
    zip_file: UploadFile = File(...),
    header_row: int = Form(2),
):
    zip_bytes = await zip_file.read()

    try:
        data = hstp_signature_list_from_zip.get_payment_headers(
            zip_bytes=zip_bytes,
            header_row=header_row,
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/signature_list/generate")
async def hstp_signature_list_generate(
    zip_file: UploadFile = File(...),
    header_row: int = Form(2),
    group_by: str = Form(...),
    output_name: str = Form("HSTP signature list"),
    top_mappings: str = Form(...),
    table_mappings: str = Form(...),
):
    zip_bytes = await zip_file.read()

    try:
        top_map = json.loads(top_mappings)
        table_map = json.loads(table_mappings)

        out_bytes = hstp_signature_list_from_zip.generate_grouped_signature_zip(
            zip_bytes=zip_bytes,
            header_row=header_row,
            group_by=group_by,
            output_base_name=output_name,
            top_mappings=top_map,
            table_mappings=table_map,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    safe_name = (output_name or "HSTP signature list").strip() or "HSTP signature list"
    if not safe_name.lower().endswith(".zip"):
        safe_name += ".zip"

    return StreamingResponse(
        iter([out_bytes]),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={safe_name}"},
    )


@app.post("/sp_signature/headers")
async def sp_signature_headers(
    zip_file: UploadFile = File(...),
    header_row: int = Form(2),
):
    zip_bytes = await zip_file.read()

    try:
        data = sp_signature_list_from_zip.get_payment_headers(
            zip_bytes=zip_bytes,
            header_row=header_row,
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/sp_signature/generate")
async def sp_signature_generate(
    zip_file: UploadFile = File(...),
    header_row: int = Form(2),
    group_by: str = Form(...),
    output_name: str = Form("SP signature list"),
    top_mappings: str = Form(...),
    table_mappings: str = Form(...),
):
    zip_bytes = await zip_file.read()

    try:
        top_map = json.loads(top_mappings)
        table_map = json.loads(table_mappings)

        out_bytes = sp_signature_list_from_zip.generate_grouped_signature_zip(
            zip_bytes=zip_bytes,
            header_row=header_row,
            group_by=group_by,
            output_base_name=output_name,
            top_mappings=top_map,
            table_mappings=table_map,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    safe_name = (output_name or "SP signature list").strip() or "SP signature list"
    if not safe_name.lower().endswith(".zip"):
        safe_name += ".zip"

    return StreamingResponse(
        iter([out_bytes]),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={safe_name}"},
    )