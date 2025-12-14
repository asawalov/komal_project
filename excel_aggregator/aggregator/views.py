import json
import pandas as pd
import numpy as np
from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from .models import UploadedExcel
from .forms import ExcelUploadForm


def clean_for_json(obj):
    """Clean data for JSON serialization - handle NaN, Inf, etc."""
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if pd.isna(obj) or np.isinf(obj):
            return None
        return obj
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        if pd.isna(obj) or np.isinf(obj):
            return None
        return float(obj)
    elif pd.isna(obj):
        return None
    return obj


def index(request):
    """Main page with file upload form."""
    form = ExcelUploadForm()
    return render(request, "aggregator/index.html", {"form": form})


@require_http_methods(["POST"])
def upload_excel(request):
    """Handle Excel file upload and return columns."""
    form = ExcelUploadForm(request.POST, request.FILES)

    if form.is_valid():
        uploaded_file = request.FILES["file"]

        # Save the uploaded file
        excel_instance = form.save(commit=False)
        excel_instance.original_filename = uploaded_file.name
        excel_instance.save()

        try:
            # Read the Excel file to get columns
            file_path = excel_instance.file.path

            if uploaded_file.name.endswith(".csv"):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)

            columns = df.columns.tolist()

            # Identify numeric columns for aggregation
            numeric_columns = df.select_dtypes(
                include=["int64", "float64", "int32", "float32", "number"]
            ).columns.tolist()

            # Get data preview (first 5 rows) - clean for JSON
            preview_df = df.head(5).fillna("")
            preview_data = clean_for_json(preview_df.to_dict("records"))

            # Store file ID in session for later use
            request.session["uploaded_file_id"] = excel_instance.id

            return JsonResponse(
                {
                    "success": True,
                    "file_id": excel_instance.id,
                    "filename": uploaded_file.name,
                    "columns": columns,
                    "numeric_columns": numeric_columns,
                    "row_count": len(df),
                    "preview": preview_data,
                }
            )

        except Exception as e:
            # Delete the file if processing fails
            excel_instance.delete()
            return JsonResponse(
                {"success": False, "error": f"Error processing file: {str(e)}"},
                status=400,
            )
    else:
        errors = form.errors.as_json()
        return JsonResponse({"success": False, "error": json.loads(errors)}, status=400)


@require_http_methods(["POST"])
def get_column_values(request):
    """Get unique values for a column (for autocomplete/filter)."""
    try:
        data = json.loads(request.body)
        file_id = data.get("file_id")
        column_name = data.get("column_name")

        if not file_id:
            return JsonResponse(
                {"success": False, "error": "No file ID provided"}, status=400
            )

        if not column_name:
            return JsonResponse(
                {"success": False, "error": "No column name provided"}, status=400
            )

        excel_instance = get_object_or_404(UploadedExcel, id=file_id)
        file_path = excel_instance.file.path

        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        if column_name not in df.columns:
            return JsonResponse(
                {"success": False, "error": f"Column '{column_name}' not found"},
                status=400,
            )

        # Get unique values
        col_series = df[column_name].dropna()
        unique_values = col_series.unique().tolist()

        # Clean values for JSON and convert to appropriate types
        cleaned_values = []
        for v in unique_values:
            cleaned = clean_for_json(v)
            # For display purposes, convert to string but keep original value info
            cleaned_values.append(cleaned)

        # Sort values (handle mixed types)
        try:
            # Sort numerically if all values are numbers
            if all(
                isinstance(v, (int, float)) and v is not None for v in cleaned_values
            ):
                cleaned_values = sorted(cleaned_values)
            else:
                cleaned_values = sorted(
                    cleaned_values,
                    key=lambda x: (x is None, str(x) if x is not None else ""),
                )
        except:
            pass

        return JsonResponse(
            {
                "success": True,
                "column": column_name,
                "values": cleaned_values,
                "count": len(cleaned_values),
                "dtype": str(df[column_name].dtype),  # Include dtype info for debugging
            }
        )

    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)


def convert_filter_values(df_column, filter_values):
    """Convert filter values to match the DataFrame column dtype."""
    col_dtype = df_column.dtype
    converted_values = []

    for val in filter_values:
        try:
            if pd.api.types.is_integer_dtype(col_dtype):
                # Convert to int
                converted_values.append(int(float(val)))
            elif pd.api.types.is_float_dtype(col_dtype):
                # Convert to float
                converted_values.append(float(val))
            elif pd.api.types.is_bool_dtype(col_dtype):
                # Convert to bool
                converted_values.append(str(val).lower() in ("true", "1", "yes"))
            else:
                # Keep as string, but also try to match original type
                converted_values.append(val)
                # Also add string version if val is numeric
                if isinstance(val, (int, float)):
                    converted_values.append(str(val))
        except (ValueError, TypeError):
            # If conversion fails, keep original value
            converted_values.append(val)

    return converted_values


@require_http_methods(["POST"])
def get_aggregation(request):
    """Perform aggregation on selected columns grouped by multiple main columns."""
    try:
        data = json.loads(request.body)
        file_id = data.get("file_id")
        group_by_columns = data.get("group_by_columns", [])  # Multiple main columns
        column_filters = data.get(
            "column_filters", {}
        )  # Filters for each column: {column: [values]}
        aggregation_columns = data.get(
            "aggregation_columns", []
        )  # Numeric columns to aggregate
        aggregation_type = data.get("aggregation_type", "sum")

        if not file_id:
            return JsonResponse(
                {"success": False, "error": "No file ID provided"}, status=400
            )

        if not group_by_columns:
            return JsonResponse(
                {"success": False, "error": "No grouping columns selected"}, status=400
            )

        if not aggregation_columns:
            return JsonResponse(
                {"success": False, "error": "No aggregation columns selected"},
                status=400,
            )

        # Get the uploaded file
        excel_instance = get_object_or_404(UploadedExcel, id=file_id)
        file_path = excel_instance.file.path

        # Read the file
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        original_rows = len(df)

        # Apply filters for each column with type conversion
        filters_applied = {}
        for col, values in column_filters.items():
            if values and len(values) > 0 and col in df.columns:
                # Convert filter values to match column dtype
                converted_values = convert_filter_values(df[col], values)

                # Apply filter - also handle string matching for mixed types
                mask = df[col].isin(converted_values)

                # If no matches found, try string comparison as fallback
                if not mask.any():
                    df_col_str = df[col].astype(str).str.strip()
                    values_str = [str(v).strip() for v in values]
                    mask = df_col_str.isin(values_str)

                df = df[mask]
                filters_applied[col] = values

        # Prepare result
        result = {
            "success": True,
            "aggregation_type": aggregation_type,
            "group_by_columns": group_by_columns,
            "aggregation_columns": aggregation_columns,
            "original_rows": original_rows,
            "filtered_rows": len(df),
            "filters_applied": filters_applied,
        }

        # Define aggregation functions
        agg_functions = {
            "sum": "sum",
            "mean": "mean",
            "count": "count",
            "min": "min",
            "max": "max",
            "median": "median",
            "std": "std",
            "var": "var",
        }

        agg_func = agg_functions.get(aggregation_type, "sum")

        # Overall stats (without grouping)
        overall_stats = {}
        for col in aggregation_columns:
            numeric_col = pd.to_numeric(df[col], errors="coerce")
            overall_stats[col] = {
                "sum": clean_for_json(numeric_col.sum()),
                "mean": clean_for_json(numeric_col.mean()),
                "count": clean_for_json(numeric_col.count()),
                "min": clean_for_json(numeric_col.min()),
                "max": clean_for_json(numeric_col.max()),
                "median": clean_for_json(numeric_col.median()),
                "std": clean_for_json(numeric_col.std()),
                "var": clean_for_json(numeric_col.var()),
            }
        result["overall_stats"] = overall_stats

        # Grouped aggregation by all selected group columns
        if len(df) > 0:
            agg_dict = {col: agg_func for col in aggregation_columns}
            grouped = df.groupby(group_by_columns).agg(agg_dict).reset_index()

            # Rename columns
            new_columns = list(group_by_columns) + [
                f"{col}_{aggregation_type}" for col in aggregation_columns
            ]
            grouped.columns = new_columns

            # Sort by first aggregation column (descending)
            if len(aggregation_columns) > 0:
                grouped = grouped.sort_values(
                    by=f"{aggregation_columns[0]}_{aggregation_type}", ascending=False
                )

            grouped_data = clean_for_json(grouped.to_dict("records"))
        else:
            grouped_data = []

        result["grouped_data"] = grouped_data
        result["group_count"] = len(grouped_data)

        return JsonResponse(result)

    except Exception as e:
        import traceback

        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)}, status=400)


@require_http_methods(["POST"])
def get_column_preview(request):
    """Get preview data for selected columns."""
    try:
        data = json.loads(request.body)
        file_id = data.get("file_id")
        selected_columns = data.get("selected_columns", [])

        if not file_id:
            return JsonResponse(
                {"success": False, "error": "No file ID provided"}, status=400
            )

        excel_instance = get_object_or_404(UploadedExcel, id=file_id)
        file_path = excel_instance.file.path

        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        if selected_columns:
            df = df[selected_columns]

        preview_df = df.head(10).fillna("")
        preview_data = clean_for_json(preview_df.to_dict("records"))

        return JsonResponse(
            {"success": True, "preview": preview_data, "total_rows": len(df)}
        )

    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)


def cleanup_file(request, file_id):
    """Delete uploaded file."""
    try:
        excel_instance = get_object_or_404(UploadedExcel, id=file_id)
        excel_instance.delete()
        return JsonResponse({"success": True})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)


# ============================================
# Myntra Product Scraper Views
# ============================================

import requests
import re
import time
import concurrent.futures


def myntra_scraper(request):
    """Myntra product scraper page."""
    return render(request, "aggregator/myntra_scraper.html")


def extract_sizes_from_html(html_content):
    """Extract sizes array from HTML using regex, including actual stock quantity."""
    sizes = []

    # First, extract basic size info with skuId
    size_pattern = r'\{"skuId":(\d+),"styleId":\d+,[^}]*"label":"([^"]+)","available":(true|false)[^}]*\}'

    for match in re.finditer(size_pattern, html_content):
        sku_id, label, available = match.groups()
        sizes.append(
            {
                "size": label,
                "available": available == "true",
                "quantity": 0,
                "price": None,
                "sku_id": sku_id,
            }
        )

    # Extract quantity by matching label positions with availableCount positions
    # Each size label is followed by its availableCount in the HTML

    # Find all labels that look like sizes (numbers or size codes)
    size_like_labels = []
    for match in re.finditer(r'"label":"([^"]+)"', html_content):
        label = match.group(1)
        # Check if it's a size-like value
        if (
            label.isdigit()
            or label
            in [
                "XS",
                "S",
                "M",
                "L",
                "XL",
                "XXL",
                "XXXL",
                "2XL",
                "3XL",
                "4XL",
                "5XL",
                "Free Size",
                "One Size",
                "FREE SIZE",
                "ONE SIZE",
            ]
            or re.match(r"^\d+[A-Z]?$", label)
        ):  # e.g., 26, 28, 32A, 34B
            size_like_labels.append((match.start(), label))

    # Find all availableCount values
    available_counts = [
        (m.start(), int(m.group(1)))
        for m in re.finditer(r'"availableCount":(\d+)', html_content)
    ]

    # Match each size label with its following availableCount
    size_quantities = {}
    for i, (label_pos, label) in enumerate(size_like_labels):
        # Find the nearest availableCount after this label
        next_label_pos = (
            size_like_labels[i + 1][0]
            if i + 1 < len(size_like_labels)
            else float("inf")
        )

        for count_pos, count in available_counts:
            if label_pos < count_pos < next_label_pos:
                size_quantities[label] = count
                break

    # Update sizes with actual quantities
    for size in sizes:
        if size["size"] in size_quantities:
            size["quantity"] = size_quantities[size["size"]]
        elif size["available"]:
            size["quantity"] = "In Stock"
        else:
            size["quantity"] = 0

    return sizes


def scrape_myntra_product(product_id):
    """Scrape a single Myntra product from their website."""
    url = f"https://www.myntra.com/{product_id}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    result = {
        "product_id": product_id,
        "url": url,
        "success": False,
        "product_name": None,
        "brand": None,
        "price": None,
        "mrp": None,
        "discount": None,
        "sizes": [],
        "error": None,
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code == 404:
            result["error"] = "Product not found"
            return result

        if response.status_code != 200:
            result["error"] = f"HTTP Error: {response.status_code}"
            return result

        html_content = response.text

        # Extract product name
        name_match = re.search(r'"pdpData":\{"id":\d+,"name":"([^"]+)"', html_content)
        if name_match:
            result["product_name"] = name_match.group(1)

        # Extract brand - try multiple patterns
        brand_patterns = [
            r'"brand":\{"uidx":"[^"]*","name":"([^"]+)"',
            r'"brand":\{"name":"([^"]+)"',
            r'"brandName":"([^"]+)"',
            r'"analytics":\{[^}]*"brand":"([^"]+)"',
        ]
        for pattern in brand_patterns:
            brand_match = re.search(pattern, html_content)
            if brand_match:
                result["brand"] = brand_match.group(1)
                break

        # Extract MRP
        mrp_match = re.search(r'"pdpData":\{[^}]*"mrp":(\d+)', html_content)
        if mrp_match:
            result["mrp"] = int(mrp_match.group(1))

        # Extract discounted price
        price_match = re.search(r'"price":\{"mrp":\d+,"discounted":(\d+)', html_content)
        if price_match:
            result["price"] = int(price_match.group(1))
        elif result["mrp"]:
            result["price"] = result["mrp"]

        # Extract discount - try to find it or calculate it
        discount_match = re.search(r'"discount":(\d+)', html_content)
        if discount_match:
            result["discount"] = int(discount_match.group(1))
        elif result["mrp"] and result["price"] and result["mrp"] > result["price"]:
            # Calculate discount percentage
            result["discount"] = round(
                ((result["mrp"] - result["price"]) / result["mrp"]) * 100
            )

        # Extract sizes
        result["sizes"] = extract_sizes_from_html(html_content)

        # Update prices in sizes
        for size in result["sizes"]:
            size["price"] = result["price"]

        # Check if we got the essential data
        if result["product_name"]:
            result["success"] = True
        else:
            # Try og:title as fallback
            og_match = re.search(
                r'<meta property="og:title" content="([^"]+)"', html_content
            )
            if og_match:
                result["product_name"] = og_match.group(1)
                result["success"] = True
            else:
                result["error"] = "Could not parse product data"

        return result

    except requests.exceptions.Timeout:
        result["error"] = "Request timeout"
        return result
    except requests.exceptions.RequestException as e:
        result["error"] = f"Request error: {str(e)}"
        return result
    except Exception as e:
        result["error"] = f"Error: {str(e)}"
        return result


@require_http_methods(["POST"])
def fetch_myntra_products(request):
    """Fetch product details for multiple Myntra product IDs with rate limiting."""
    try:
        data = json.loads(request.body)
        product_ids = data.get("product_ids", [])

        if not product_ids:
            return JsonResponse(
                {"success": False, "error": "No product IDs provided"}, status=400
            )

        # No hard limit - but we'll process in batches to avoid IP blocking
        # Clean and validate product IDs
        cleaned_ids = []
        for pid in product_ids:
            # Extract numeric ID if full URL is provided
            pid_str = str(pid).strip()
            # Handle full URLs like www.myntra.com/38461906
            match = re.search(r"(\d{6,})", pid_str)
            if match:
                cleaned_ids.append(match.group(1))
            elif pid_str.isdigit():
                cleaned_ids.append(pid_str)

        if not cleaned_ids:
            return JsonResponse(
                {"success": False, "error": "No valid product IDs found"}, status=400
            )

        # Remove duplicates while preserving order
        cleaned_ids = list(dict.fromkeys(cleaned_ids))

        results = []
        errors = []

        # Rate limiting settings
        BATCH_SIZE = 50  # Process 50 products at a time
        DELAY_BETWEEN_BATCHES = 5  # 5 seconds delay between batches
        MAX_WORKERS = 10  # Max concurrent requests within a batch

        # Split into batches
        batches = [
            cleaned_ids[i : i + BATCH_SIZE]
            for i in range(0, len(cleaned_ids), BATCH_SIZE)
        ]
        total_batches = len(batches)

        for batch_index, batch in enumerate(batches):
            # Process batch with ThreadPoolExecutor
            max_workers = min(MAX_WORKERS, len(batch))

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_to_id = {
                    executor.submit(scrape_myntra_product, pid): pid for pid in batch
                }

                for future in concurrent.futures.as_completed(future_to_id):
                    pid = future_to_id[future]
                    try:
                        result = future.result()
                        results.append(result)
                        if not result["success"]:
                            errors.append(
                                {
                                    "product_id": pid,
                                    "error": result.get("error", "Unknown error"),
                                }
                            )
                    except Exception as e:
                        errors.append({"product_id": pid, "error": str(e)})
                        results.append(
                            {"product_id": pid, "success": False, "error": str(e)}
                        )

            # Add delay between batches to avoid IP blocking (but not after the last batch)
            if batch_index < total_batches - 1:
                print(
                    f"Completed batch {batch_index + 1}/{total_batches}. Waiting {DELAY_BETWEEN_BATCHES}s before next batch..."
                )
                time.sleep(DELAY_BETWEEN_BATCHES)

        # Sort results by product_id to maintain order
        results.sort(key=lambda x: cleaned_ids.index(x["product_id"]))

        return JsonResponse(
            {
                "success": True,
                "total_requested": len(cleaned_ids),
                "total_success": sum(1 for r in results if r["success"]),
                "total_failed": sum(1 for r in results if not r["success"]),
                "products": results,
                "errors": errors,
            }
        )

    except Exception as e:
        import traceback

        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)}, status=400)


# ============================================
# Excel Merge Functionality
# ============================================


def excel_merge(request):
    """Render the Excel merge page."""
    return render(request, "aggregator/excel_merge.html")


@require_http_methods(["POST"])
def upload_merge_excel(request):
    """Handle Excel file upload for merging and return columns."""
    try:
        uploaded_file = request.FILES.get("file")
        file_type = request.POST.get("file_type", "child")  # 'main' or 'child'

        if not uploaded_file:
            return JsonResponse(
                {"success": False, "error": "No file provided"}, status=400
            )

        # Read the file into a DataFrame
        file_ext = uploaded_file.name.split(".")[-1].lower()

        if file_ext == "csv":
            df = pd.read_csv(uploaded_file)
        elif file_ext in ["xlsx", "xls"]:
            df = pd.read_excel(uploaded_file)
        else:
            return JsonResponse(
                {
                    "success": False,
                    "error": "Unsupported file format. Use .xlsx, .xls, or .csv",
                },
                status=400,
            )

        # Save the file temporarily
        excel_instance = UploadedExcel.objects.create(
            file=uploaded_file, original_filename=uploaded_file.name
        )

        # Get all columns
        columns = df.columns.tolist()

        # Get preview data (first 5 rows)
        preview_data = df.head(5).to_dict(orient="records")
        preview_data = clean_for_json(preview_data)

        return JsonResponse(
            {
                "success": True,
                "file_id": excel_instance.id,
                "filename": uploaded_file.name,
                "file_type": file_type,
                "columns": columns,
                "row_count": len(df),
                "preview": preview_data,
            }
        )

    except Exception as e:
        import traceback

        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)}, status=400)


@require_http_methods(["POST"])
def perform_merge(request):
    """Perform the merge operation on uploaded Excel files."""
    try:
        data = json.loads(request.body)

        main_file_id = data.get("main_file_id")
        main_columns = data.get("main_columns", [])
        child_files = data.get("child_files", [])  # List of {file_id, columns}
        
        # Priority merge options
        use_priority_merge = data.get("priority_merge", False)
        primary_column = data.get("primary_column")
        fallback_column = data.get("fallback_column")
        
        # Support both single column (legacy) and multiple columns
        merge_columns = data.get("merge_columns", [])
        if not merge_columns and not use_priority_merge:
            # Fallback to single merge_column for backwards compatibility
            single_col = data.get("merge_column")
            if single_col:
                merge_columns = [single_col]
        
        merge_type = data.get("merge_type", "left")  # left, right, outer, inner
        remove_na = data.get("remove_na", False)  # Remove NA rows from child files
        drop_duplicates = data.get(
            "drop_duplicates", False
        )  # Drop duplicate rows from child files

        if not main_file_id:
            return JsonResponse(
                {"success": False, "error": "Main file is required"}, status=400
            )

        if use_priority_merge:
            if not primary_column or not fallback_column:
                return JsonResponse(
                    {"success": False, "error": "Both primary and fallback columns are required for priority merge"},
                    status=400,
                )
            # For priority merge, we'll use both columns in the process
            merge_columns = [primary_column]  # Start with primary for column selection
        elif not merge_columns:
            return JsonResponse(
                {"success": False, "error": "At least one merge column is required"},
                status=400,
            )

        # Load main file
        main_excel = get_object_or_404(UploadedExcel, id=main_file_id)
        main_path = main_excel.file.path
        file_ext = main_path.split(".")[-1].lower()

        if file_ext == "csv":
            main_df = pd.read_csv(main_path)
        else:
            main_df = pd.read_excel(main_path)

        # Determine all columns needed for merging
        if use_priority_merge:
            all_merge_cols = [primary_column, fallback_column]
        else:
            all_merge_cols = merge_columns
        
        # Select only specified columns from main file (always include merge columns)
        if main_columns:
            columns_to_keep = list(set(main_columns + all_merge_cols))
            columns_to_keep = [c for c in columns_to_keep if c in main_df.columns]
            main_df = main_df[columns_to_keep]

        # Apply renames to main file
        main_renames = data.get("main_renames", {})
        if main_renames:
            main_df = main_df.rename(columns=main_renames)
            # Update merge columns if any were renamed
            if use_priority_merge:
                primary_column = main_renames.get(primary_column, primary_column)
                fallback_column = main_renames.get(fallback_column, fallback_column)
            else:
                merge_columns = [main_renames.get(c, c) for c in merge_columns]

        result_df = main_df.copy()

        # First, merge all child files together
        child_dfs = []
        for child_info in child_files:
            child_file_id = child_info.get("file_id")
            child_columns = child_info.get("columns", [])

            if not child_file_id:
                continue

            child_excel = get_object_or_404(UploadedExcel, id=child_file_id)
            child_path = child_excel.file.path
            child_ext = child_path.split(".")[-1].lower()

            if child_ext == "csv":
                child_df = pd.read_csv(child_path)
            else:
                child_df = pd.read_excel(child_path)

            # Select only specified columns (always include merge columns)
            cols_for_merge = all_merge_cols if use_priority_merge else merge_columns
            if child_columns:
                columns_to_keep = list(set(child_columns + cols_for_merge))
                columns_to_keep = [c for c in columns_to_keep if c in child_df.columns]
                child_df = child_df[columns_to_keep]

            # Apply renames to child file
            child_renames = child_info.get("renames", {})
            if child_renames:
                child_df = child_df.rename(columns=child_renames)

            # Remove rows with NA values if option is enabled
            if remove_na:
                child_df = child_df.dropna()

            # Remove duplicate rows based on merge columns if option is enabled
            if drop_duplicates:
                # Get renamed merge columns for this child
                renamed_merge_cols = [child_renames.get(c, c) for c in cols_for_merge]
                available_merge_cols = [
                    c for c in renamed_merge_cols if c in child_df.columns
                ]
                if available_merge_cols:
                    child_df = child_df.drop_duplicates(
                        subset=available_merge_cols, keep="first"
                    )

            child_dfs.append(child_df)

        # Merge all child DataFrames first (if multiple)
        if use_priority_merge:
            # For priority merge, use primary column first
            child_merge_col = primary_column
        else:
            child_merge_col = merge_columns
        
        if len(child_dfs) > 1:
            merged_children = child_dfs[0]
            for i, cdf in enumerate(child_dfs[1:], start=1):
                # Handle duplicate columns by adding suffix
                merge_on = child_merge_col if isinstance(child_merge_col, list) else [child_merge_col]
                merged_children = pd.merge(
                    merged_children,
                    cdf,
                    on=merge_on,
                    how="outer",
                    suffixes=("", f"_child{i + 1}"),
                )
        elif len(child_dfs) == 1:
            merged_children = child_dfs[0]
        else:
            merged_children = None

        # Merge with main file
        if merged_children is not None:
            if use_priority_merge:
                # Priority merge: First try primary column, then fallback
                # Step 1: Merge on primary column
                primary_merge = pd.merge(
                    result_df,
                    merged_children,
                    on=primary_column,
                    how="left",
                    suffixes=("", "_child"),
                    indicator=True
                )
                
                # Get child columns (excluding merge columns)
                child_data_cols = [c for c in merged_children.columns if c not in [primary_column, fallback_column]]
                
                # Step 2: Find rows that didn't match on primary
                unmatched_mask = primary_merge['_merge'] == 'left_only'
                
                if unmatched_mask.any() and fallback_column in result_df.columns and fallback_column in merged_children.columns:
                    # Get unmatched rows from main
                    unmatched_main = result_df[result_df[primary_column].isin(primary_merge.loc[unmatched_mask, primary_column])]
                    
                    # Try to merge unmatched rows on fallback column
                    fallback_merge = pd.merge(
                        unmatched_main,
                        merged_children,
                        on=fallback_column,
                        how="left",
                        suffixes=("", "_fallback")
                    )
                    
                    # Update the primary merge result with fallback matches
                    # For each child column, fill NA values with fallback values
                    for col in child_data_cols:
                        child_col = col if col in primary_merge.columns else f"{col}_child"
                        fallback_col = col if col in fallback_merge.columns else f"{col}_fallback"
                        
                        if child_col in primary_merge.columns and fallback_col in fallback_merge.columns:
                            # Create a mapping from main index to fallback values
                            fallback_values = fallback_merge.set_index(unmatched_main.index)[fallback_col]
                            primary_merge.loc[unmatched_mask, child_col] = primary_merge.loc[unmatched_mask, child_col].fillna(
                                fallback_values.reindex(primary_merge.loc[unmatched_mask].index)
                            )
                
                # Remove the merge indicator column
                result_df = primary_merge.drop(columns=['_merge'])
            else:
                # Regular merge
                result_df = pd.merge(
                    result_df,
                    merged_children,
                    on=merge_columns,
                    how=merge_type,
                    suffixes=("_main", "_child"),
                )

        # Clean the result
        result_df = result_df.fillna("")

        # Convert to records for preview
        preview_data = result_df.head(100).to_dict(orient="records")
        preview_data = clean_for_json(preview_data)

        # Get column info
        result_columns = result_df.columns.tolist()

        # Save the merged file
        import os
        from django.conf import settings
        import uuid

        merged_filename = f"merged_{uuid.uuid4().hex[:8]}.xlsx"
        merged_path = os.path.join(settings.MEDIA_ROOT, "merged", merged_filename)

        # Ensure directory exists
        os.makedirs(os.path.dirname(merged_path), exist_ok=True)

        # Save to Excel
        result_df.to_excel(merged_path, index=False)

        return JsonResponse(
            {
                "success": True,
                "total_rows": len(result_df),
                "total_columns": len(result_columns),
                "columns": result_columns,
                "preview": preview_data,
                "download_url": f"/media/merged/{merged_filename}",
            }
        )

    except Exception as e:
        import traceback

        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)}, status=400)
