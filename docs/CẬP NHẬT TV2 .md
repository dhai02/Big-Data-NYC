# Tong Hop Sua Doi (Session)

Ngay cap nhat: 2026-03-30

## 1) Hoan thien phan TV2 theo de

- Cap nhat EDA trong `src/01_eda.py`:
- Kiem tra duplicate.
- Phan tich phan phoi `trip_distance` va `fare_amount`.
- Tong hop data issues trong output.

- Cap nhat cleaning trong `src/02_data_cleaning.py`:
- Outlier tuning voi nhieu multiplier IQR (`1.5, 2.0, 2.5, 3.0`).
- Chon multiplier tu dong theo retain ratio.
- Giu feature engineering can thiet (`trip_duration_min`, `tip_percentage`, `speed_mph`, ...).

- Them utility tach rieng de de test:
- `src/cleaning_utils.py`

## 2) Bo sung tests theo checklist

- Them `tests/test_cleaning_utils.py`.
- Them `tests/test_cleaning.py`.
- Them `tests/test_aggregation.py`.

Muc tieu:
- Co unit tests cho cleaning/outlier.
- Co unit tests cho logic aggregation co ban.

## 3) Bo sung notebook theo khung de

- Them `notebooks/02_cleaning_demo.ipynb`.
- Them `notebooks/03_aggregation_viz.ipynb`.

Cap nhat notebook theo huong:
- Doc truc tiep tu HDFS (`hdfs://namenode:9000/...`), khong dung `localhost`.
- Tranh phu thuoc vao report JSON.

## 4) Cap nhat Docker de chay test/notebook day du

- Sua `docker-compose.yml` de mount them:
- `./tests:/app/tests`
- `./docs:/app/docs`

Ket qua:
- Co the chay test trong container:
- `docker exec spark python3 -m pytest -q /app/tests/test_cleaning_utils.py`

## 5) Bo huong report JSON (theo yeu cau)

- Da bo code sinh report JSON trong:
- `src/01_eda.py`
- `src/02_data_cleaning.py`

- Da xoa thu muc `data/reports`.
- Da chinh lai `README.md` theo huong demo truc quan bang notebook.

## 6) Luu y van hanh

- Trong container, dung `python3` (khong dung `python`).
- HDFS endpoint trong container la `namenode:9000` (khong dung `localhost:9000`).

## 7) Danh sach file chinh da tac dong

- `README.md`
- `docker-compose.yml`
- `src/01_eda.py`
- `src/02_data_cleaning.py`
- `src/cleaning_utils.py`
- `tests/test_cleaning_utils.py`
- `tests/test_cleaning.py`
- `tests/test_aggregation.py`
- `notebooks/02_cleaning_demo.ipynb`
- `notebooks/03_aggregation_viz.ipynb`
