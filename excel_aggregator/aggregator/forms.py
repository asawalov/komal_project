from django import forms
from .models import UploadedExcel


class ExcelUploadForm(forms.ModelForm):
    """Form for uploading Excel files."""
    
    class Meta:
        model = UploadedExcel
        fields = ['file']
        widgets = {
            'file': forms.FileInput(attrs={
                'accept': '.xlsx,.xls,.csv',
                'class': 'file-input',
                'id': 'excel-file-input'
            })
        }
    
    def clean_file(self):
        file = self.cleaned_data.get('file')
        if file:
            # Check file extension
            ext = file.name.split('.')[-1].lower()
            if ext not in ['xlsx', 'xls', 'csv']:
                raise forms.ValidationError(
                    'Only Excel files (.xlsx, .xls) and CSV files are allowed.'
                )
            # Check file size (max 10MB)
            if file.size > 10 * 1024 * 1024:
                raise forms.ValidationError(
                    'File size must be less than 10MB.'
                )
        return file

