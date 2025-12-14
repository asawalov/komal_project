from django.db import models
import os


def excel_upload_path(instance, filename):
    """Generate upload path for Excel files."""
    return f'excel_files/{filename}'


class UploadedExcel(models.Model):
    """Model to store uploaded Excel files."""
    file = models.FileField(upload_to=excel_upload_path)
    original_filename = models.CharField(max_length=255)
    uploaded_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-uploaded_at']
    
    def __str__(self):
        return f"{self.original_filename} - {self.uploaded_at}"
    
    def delete(self, *args, **kwargs):
        """Delete the file when the model instance is deleted."""
        if self.file:
            if os.path.isfile(self.file.path):
                os.remove(self.file.path)
        super().delete(*args, **kwargs)

