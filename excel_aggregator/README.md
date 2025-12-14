# Excel Data Aggregator

A Django web application that allows you to upload Excel files, dynamically select columns, and perform various aggregations on your data.

## Features

- 📁 **File Upload**: Drag & drop or click to upload Excel files (.xlsx, .xls) or CSV files
- 📊 **Dynamic Column Selection**: Automatically detects all columns from your uploaded file
- 🔢 **Numeric Column Detection**: Automatically identifies numeric columns for aggregation
- 📈 **Multiple Aggregation Types**: Sum, Mean, Count, Min, Max, Median, Std Dev, Variance
- 🗂️ **Group By Support**: Optionally group your aggregations by any column
- 👁️ **Data Preview**: Preview your data before running aggregations
- 🎨 **Beautiful UI**: Modern, dark-themed interface with smooth animations

## Installation

1. **Navigate to the project directory:**
   ```bash
   cd excel_aggregator
   ```

2. **Create a virtual environment (recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r ../requirements.txt
   ```

4. **Run database migrations:**
   ```bash
   python manage.py makemigrations
   python manage.py migrate
   ```

5. **Start the development server:**
   ```bash
   python manage.py runserver
   ```

6. **Open your browser and visit:**
   ```
   http://127.0.0.1:8000/
   ```

## Usage

1. **Upload a File**: Drag and drop your Excel file onto the upload zone, or click to browse
2. **Select Columns**: Check/uncheck the columns you want to include in your analysis
3. **Configure Aggregation**:
   - Select the numeric column you want to aggregate
   - Optionally select a column to group by
   - Choose the aggregation type (Sum, Mean, Count, etc.)
4. **View Results**: See your aggregation results with all statistical metrics

## Project Structure

```
excel_aggregator/
├── excel_aggregator/       # Django project settings
│   ├── settings.py
│   ├── urls.py
│   └── wsgi.py
├── aggregator/             # Main application
│   ├── models.py           # Database models
│   ├── views.py            # View functions
│   ├── forms.py            # Form definitions
│   ├── urls.py             # URL routing
│   └── admin.py            # Admin configuration
├── templates/              # HTML templates
│   ├── base.html
│   └── aggregator/
│       └── index.html
├── static/                 # Static files
├── media/                  # Uploaded files
├── manage.py
└── README.md
```

## API Endpoints

- `GET /` - Main page with file upload form
- `POST /upload/` - Upload Excel file and get columns
- `POST /aggregate/` - Run aggregation on selected data
- `POST /preview/` - Get preview of selected columns
- `POST /cleanup/<file_id>/` - Delete uploaded file

## Technologies Used

- **Backend**: Django 4.2
- **Data Processing**: Pandas, NumPy, openpyxl
- **Frontend**: Vanilla JavaScript, Custom CSS
- **Database**: SQLite (default)

## License

This project is open source and available under the MIT License.

