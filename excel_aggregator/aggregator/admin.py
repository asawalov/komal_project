from django.contrib import admin
from .models import UploadedExcel


@admin.register(UploadedExcel)
class UploadedExcelAdmin(admin.ModelAdmin):
    list_display = ['original_filename', 'uploaded_at']
    list_filter = ['uploaded_at']
    search_fields = ['original_filename']

