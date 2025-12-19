from django.urls import path
from . import views

app_name = "aggregator"

urlpatterns = [
    path("", views.index, name="index"),
    path("upload/", views.upload_excel, name="upload"),
    path("column-values/", views.get_column_values, name="column_values"),
    path("aggregate/", views.get_aggregation, name="aggregate"),
    path("preview/", views.get_column_preview, name="preview"),
    path("cleanup/<int:file_id>/", views.cleanup_file, name="cleanup"),
    # Myntra Scraper
    path("myntra/", views.myntra_scraper, name="myntra_scraper"),
    path("myntra/fetch/", views.fetch_myntra_products, name="fetch_myntra"),
    # Excel Merge
    path("merge/", views.excel_merge, name="excel_merge"),
    path("merge/upload/", views.upload_merge_excel, name="upload_merge"),
    path("merge/perform/", views.perform_merge, name="perform_merge"),
    # Formula Columns
    path("formula/", views.formula_columns, name="formula_columns"),
    path("formula/upload/", views.formula_upload, name="formula_upload"),
    path("formula/validate/", views.formula_validate, name="formula_validate"),
    path("formula/execute/", views.formula_execute, name="formula_execute"),
]
