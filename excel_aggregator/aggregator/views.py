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

        # Get unique values in order of first appearance in Excel
        col_series = df[column_name].dropna()
        # Use dict.fromkeys to preserve order of first occurrence (Python 3.7+)
        unique_values = list(dict.fromkeys(col_series.tolist()))

        # Clean values for JSON and convert to appropriate types
        cleaned_values = []
        for v in unique_values:
            cleaned = clean_for_json(v)
            # For display purposes, convert to string but keep original value info
            cleaned_values.append(cleaned)

        # Keep original Excel order - no sorting
        # Values appear in the order they first appear in the spreadsheet

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


def get_free_proxy():
    """Fetch a free proxy from public proxy lists."""
    import random

    free_proxy_apis = [
        # ProxyScrape free API
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
        # Free proxy list
        "https://www.proxy-list.download/api/v1/get?type=http",
    ]

    for api_url in free_proxy_apis:
        try:
            response = requests.get(api_url, timeout=10)
            if response.status_code == 200:
                proxies = response.text.strip().split("\n")
                # Filter out empty lines and get working proxies
                proxies = [p.strip() for p in proxies if p.strip() and ":" in p]
                if proxies:
                    # Return a random proxy from the list
                    proxy = random.choice(proxies[:20])  # Use from top 20
                    return f"http://{proxy}"
        except Exception:
            continue

    return None


# Cache for free proxies (to avoid fetching too often)
_free_proxy_cache = {
    "proxies": [],
    "last_fetch": 0,
    "cache_duration": 300,  # 5 minutes
}


def get_cached_free_proxy():
    """Get a free proxy with caching to avoid too many API calls."""
    import random

    current_time = time.time()

    # Refresh cache if expired or empty
    if (
        not _free_proxy_cache["proxies"]
        or (current_time - _free_proxy_cache["last_fetch"])
        > _free_proxy_cache["cache_duration"]
    ):
        try:
            # Fetch from ProxyScrape
            response = requests.get(
                "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=elite",
                timeout=10,
            )
            if response.status_code == 200:
                proxies = response.text.strip().split("\n")
                proxies = [p.strip() for p in proxies if p.strip() and ":" in p]
                if proxies:
                    _free_proxy_cache["proxies"] = proxies[:50]  # Keep top 50
                    _free_proxy_cache["last_fetch"] = current_time
        except Exception:
            pass

    if _free_proxy_cache["proxies"]:
        proxy = random.choice(_free_proxy_cache["proxies"])
        return f"http://{proxy}"

    return None


def get_proxy_config():
    """Get proxy configuration from environment variables or use free proxies."""
    # PROXY DISABLED - Using direct local IP for now
    # Uncomment below code to enable proxy support

    proxy_config = {
        "enabled": False,
        "type": None,
        "proxies": None,
        "scraper_api_key": None,
    }

    # Return disabled config (direct connection)
    return proxy_config

    # ========== COMMENTED OUT PROXY CODE ==========
    # import os
    #
    # # Check for ScraperAPI key (recommended - has free tier of 1000 credits/month)
    # # Hardcoded key or from environment variable
    # scraper_api_key = (
    #     os.environ.get("SCRAPER_API_KEY") or "e4465645235d5c3c6baf15af85aa1608"
    # )
    # if scraper_api_key:
    #     proxy_config["enabled"] = True
    #     proxy_config["type"] = "scraper_api"
    #     proxy_config["scraper_api_key"] = scraper_api_key
    #     return proxy_config
    #
    # # Check for custom proxy URL
    # # Format: http://user:pass@proxy.example.com:8080
    # proxy_url = os.environ.get("PROXY_URL")
    # if proxy_url:
    #     proxy_config["enabled"] = True
    #     proxy_config["type"] = "custom"
    #     proxy_config["proxies"] = {
    #         "http": proxy_url,
    #         "https": proxy_url,
    #     }
    #     return proxy_config
    #
    # # Check for rotating proxy service (like ProxyMesh, Oxylabs, etc.)
    # proxy_host = os.environ.get("PROXY_HOST")
    # proxy_port = os.environ.get("PROXY_PORT")
    # proxy_user = os.environ.get("PROXY_USER")
    # proxy_pass = os.environ.get("PROXY_PASS")
    #
    # if proxy_host and proxy_port:
    #     proxy_config["enabled"] = True
    #     proxy_config["type"] = "rotating"
    #     if proxy_user and proxy_pass:
    #         proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}"
    #     else:
    #         proxy_url = f"http://{proxy_host}:{proxy_port}"
    #     proxy_config["proxies"] = {
    #         "http": proxy_url,
    #         "https": proxy_url,
    #     }
    #     return proxy_config
    #
    # # Check if free proxy fallback is enabled (USE_FREE_PROXY=true)
    # use_free_proxy = os.environ.get("USE_FREE_PROXY", "false").lower() in (
    #     "true",
    #     "1",
    #     "yes",
    # )
    # if use_free_proxy:
    #     free_proxy = get_cached_free_proxy()
    #     if free_proxy:
    #         proxy_config["enabled"] = True
    #         proxy_config["type"] = "free_proxy"
    #         proxy_config["proxies"] = {
    #             "http": free_proxy,
    #             "https": free_proxy,
    #         }
    #         return proxy_config
    #
    # return proxy_config


def scrape_myntra_product(product_id):
    """Scrape a single Myntra product using their API."""
    import random

    result = {
        "product_id": product_id,
        "url": f"https://www.myntra.com/{product_id}",
        "success": False,
        "product_name": None,
        "brand": None,
        "price": None,
        "mrp": None,
        "discount": None,
        "sizes": [],
        "error": None,
    }

    # Proxy disabled - using direct local IP connection
    # proxy_config = get_proxy_config()  # Commented out - proxy disabled

    # Try multiple approaches
    methods_tried = []

    # Method 1: Try Myntra's product API directly
    api_url = f"https://www.myntra.com/gateway/v2/product/{product_id}"

    # ScraperAPI code commented out (proxy disabled)
    # scraper_api_params = None
    # if proxy_config["enabled"] and proxy_config["type"] == "scraper_api":
    #     api_url = "https://api.scraperapi.com/"
    #     scraper_api_params = {
    #         "api_key": proxy_config["scraper_api_key"],
    #         "url": original_api_url,
    #     }

    # Rotate user agents to avoid detection
    user_agents = [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.130 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ]

    api_headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://www.myntra.com/{product_id}",
        "Origin": "https://www.myntra.com",
        "Connection": "keep-alive",
        "x-location-context": "pincode=110001;source=IP",
        "x-myntra-app": "desktop",
        "x-requested-with": "browser",
    }

    # Proxy disabled - no proxies configured
    # proxies = None
    # if proxy_config["enabled"] and proxy_config["type"] in [
    #     "custom",
    #     "rotating",
    #     "free_proxy",
    # ]:
    #     proxies = proxy_config["proxies"]

    # Timeout for direct requests (proxy disabled)
    api_timeout = 30

    try:
        # Try API first (direct connection - proxy disabled)
        methods_tried.append("API")
        response = requests.get(api_url, headers=api_headers, timeout=api_timeout)

        if response.status_code == 200:
            try:
                api_data = response.json()
                if api_data and "style" in api_data:
                    style = api_data["style"]
                    result["product_name"] = style.get("name")
                    result["brand"] = (
                        style.get("brand", {}).get("name")
                        if isinstance(style.get("brand"), dict)
                        else style.get("brandName")
                    )

                    price_info = style.get("price", {})
                    result["mrp"] = price_info.get("mrp")
                    result["price"] = price_info.get("discounted") or result["mrp"]
                    result["discount"] = price_info.get("discount", 0)

                    # Get sizes
                    sizes_data = style.get("sizes", [])
                    for size_info in sizes_data:
                        if isinstance(size_info, dict):
                            qty = (
                                size_info.get("inventory", {}).get("quantity")
                                if isinstance(size_info.get("inventory"), dict)
                                else None
                            )
                            if qty is None:
                                qty = size_info.get("availableCount")
                            result["sizes"].append(
                                {
                                    "size": size_info.get("label", ""),
                                    "available": size_info.get("available", False),
                                    "quantity": qty
                                    if qty is not None
                                    else (
                                        "In Stock" if size_info.get("available") else 0
                                    ),
                                    "price": result["price"],
                                }
                            )

                    if result["product_name"]:
                        result["success"] = True
                        return result
            except json.JSONDecodeError:
                pass

        # Method 2: Try webpage with session (direct connection - proxy disabled)
        methods_tried.append("Webpage")
        session = requests.Session()

        # First visit homepage to get cookies
        homepage_headers = {
            "User-Agent": random.choice(user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        try:
            session.get("https://www.myntra.com/", headers=homepage_headers, timeout=10)
        except Exception:
            pass  # Ignore homepage errors, continue with product page

        # Now try the product page with cookies
        url = f"https://www.myntra.com/{product_id}"

        page_headers = {
            "User-Agent": random.choice(user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Referer": "https://www.myntra.com/",
        }

        page_timeout = 60
        response = session.get(url, headers=page_headers, timeout=page_timeout)

        if response.status_code == 404:
            result["error"] = "Product not found"
            return result

        if response.status_code != 200:
            result["error"] = f"HTTP Error: {response.status_code}"
            return result

        html_content = response.text

        # Try to extract JSON data from script tag
        pdp_data = None

        # Method 1: Look for window.__myx JSON
        myx_match = re.search(
            r"window\.__myx\s*=\s*(\{.*?\});?\s*</script>", html_content, re.DOTALL
        )
        if myx_match:
            try:
                myx_data = json.loads(myx_match.group(1))
                if "pdpData" in myx_data:
                    pdp_data = myx_data["pdpData"]
            except json.JSONDecodeError:
                pass

        # Method 2: Look for pdpData directly in script
        pdp_match_found = False
        pdp_json_error = None
        if not pdp_data:
            pdp_match = re.search(r'"pdpData"\s*:\s*(\{[^<]+\})\s*[,}]', html_content)
            if pdp_match:
                pdp_match_found = True
                try:
                    # Try to extract just the pdpData object
                    pdp_str = pdp_match.group(1)
                    # Find matching closing brace
                    brace_count = 0
                    end_idx = 0
                    for i, char in enumerate(pdp_str):
                        if char == "{":
                            brace_count += 1
                        elif char == "}":
                            brace_count -= 1
                            if brace_count == 0:
                                end_idx = i + 1
                                break
                    if end_idx > 0:
                        pdp_data = json.loads(pdp_str[:end_idx])
                except json.JSONDecodeError as e:
                    pdp_json_error = str(e)

        # Track parsing details for error reporting
        parsing_details = {
            "found_myx_data": myx_match is not None,
            "found_pdp_match": pdp_match_found,
            "pdp_json_error": pdp_json_error,
            "found_pdp_data": pdp_data is not None,
            "html_length": len(html_content),
            "has_script_tags": "<script" in html_content,
            "response_snippet": html_content[:500]
            if len(html_content) < 5000
            else None,
        }

        # Extract data from pdpData if found
        if pdp_data:
            parsing_details["pdp_keys"] = list(pdp_data.keys())[
                :20
            ]  # First 20 keys for debugging

            # Try multiple ways to get product name
            result["product_name"] = (
                pdp_data.get("name")
                or pdp_data.get("productName")
                or pdp_data.get("title")
                or pdp_data.get("product_name")
            )

            # Try to get name from nested style object
            if not result["product_name"] and "style" in pdp_data:
                style = pdp_data.get("style", {})
                if isinstance(style, dict):
                    result["product_name"] = (
                        style.get("name")
                        or style.get("productName")
                        or style.get("title")
                    )

            # Get brand - try multiple locations
            brand_info = pdp_data.get("brand", {})
            if isinstance(brand_info, dict):
                result["brand"] = (
                    brand_info.get("name")
                    or brand_info.get("brandName")
                    or brand_info.get("label")
                )
            elif isinstance(brand_info, str):
                result["brand"] = brand_info

            # Try brand from style object
            if not result["brand"] and "style" in pdp_data:
                style = pdp_data.get("style", {})
                if isinstance(style, dict):
                    brand_from_style = style.get("brand", {})
                    if isinstance(brand_from_style, dict):
                        result["brand"] = brand_from_style.get(
                            "name"
                        ) or brand_from_style.get("brandName")
                    elif isinstance(brand_from_style, str):
                        result["brand"] = brand_from_style
                    elif isinstance(style.get("brandName"), str):
                        result["brand"] = style.get("brandName")

            # Get price info - try multiple locations
            price_info = pdp_data.get("price", {})
            if isinstance(price_info, dict):
                result["mrp"] = price_info.get("mrp") or price_info.get("MRP")
                result["price"] = (
                    price_info.get("discounted")
                    or price_info.get("sellingPrice")
                    or price_info.get("price")
                    or result["mrp"]
                )
                result["discount"] = price_info.get("discount", 0) or price_info.get(
                    "discountPercent", 0
                )

            # Try price from style object
            if not result["mrp"] and "style" in pdp_data:
                style = pdp_data.get("style", {})
                if isinstance(style, dict):
                    style_price = style.get("price", {})
                    if isinstance(style_price, dict):
                        result["mrp"] = style_price.get("mrp") or style_price.get("MRP")
                        result["price"] = (
                            style_price.get("discounted")
                            or style_price.get("sellingPrice")
                            or result["mrp"]
                        )
                        result["discount"] = style_price.get(
                            "discount", 0
                        ) or style_price.get("discountPercent", 0)

            # Get sizes from pdpData - try multiple locations
            sizes_data = pdp_data.get("sizes", [])

            # Try sizes from style object if not found
            if not sizes_data and "style" in pdp_data:
                style = pdp_data.get("style", {})
                if isinstance(style, dict):
                    sizes_data = style.get("sizes", [])

            for size_info in sizes_data:
                if isinstance(size_info, dict):
                    size_entry = {
                        "size": (
                            size_info.get("label")
                            or size_info.get("size")
                            or size_info.get("name")
                            or ""
                        ),
                        "available": size_info.get("available", False),
                        "quantity": None,
                        "price": result["price"],
                    }

                    # Try multiple ways to get quantity
                    inventory = size_info.get("inventory", {})
                    if isinstance(inventory, dict):
                        size_entry["quantity"] = (
                            inventory.get("quantity")
                            or inventory.get("availableCount")
                            or inventory.get("stock")
                        )

                    if size_entry["quantity"] is None:
                        size_entry["quantity"] = (
                            size_info.get("availableCount")
                            or size_info.get("quantity")
                            or size_info.get("stock")
                        )

                    if size_entry["quantity"] is None and size_entry["available"]:
                        size_entry["quantity"] = "In Stock"
                    elif size_entry["quantity"] is None:
                        size_entry["quantity"] = 0

                    result["sizes"].append(size_entry)

        # Fallback: Try regex patterns if JSON parsing failed
        if not result["product_name"]:
            # Try multiple name patterns
            name_patterns = [
                r'"name"\s*:\s*"([^"]+)"',
                r'<h1[^>]*class="[^"]*title[^"]*"[^>]*>([^<]+)</h1>',
                r'<meta property="og:title" content="([^"]+)"',
                r'"productName"\s*:\s*"([^"]+)"',
            ]
            for pattern in name_patterns:
                match = re.search(pattern, html_content)
                if match:
                    result["product_name"] = match.group(1).strip()
                    break

        if not result["brand"]:
            brand_patterns = [
                r'"brand"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"',
                r'"brandName"\s*:\s*"([^"]+)"',
                r'<a[^>]*class="[^"]*brand[^"]*"[^>]*>([^<]+)</a>',
            ]
            for pattern in brand_patterns:
                match = re.search(pattern, html_content)
                if match:
                    result["brand"] = match.group(1).strip()
                    break

        if not result["mrp"]:
            mrp_match = re.search(r'"mrp"\s*:\s*(\d+)', html_content)
            if mrp_match:
                result["mrp"] = int(mrp_match.group(1))

        if not result["price"]:
            price_match = re.search(r'"discounted"\s*:\s*(\d+)', html_content)
            if price_match:
                result["price"] = int(price_match.group(1))
            elif result["mrp"]:
                result["price"] = result["mrp"]

        # Calculate discount if not found
        if (
            not result["discount"]
            and result["mrp"]
            and result["price"]
            and result["mrp"] > result["price"]
        ):
            result["discount"] = round(
                ((result["mrp"] - result["price"]) / result["mrp"]) * 100
            )

        # Extract sizes if not already found
        if not result["sizes"]:
            result["sizes"] = extract_sizes_from_html(html_content)
            for size in result["sizes"]:
                size["price"] = result["price"]

        # Clean up product_name (strip whitespace, handle None)
        if result["product_name"]:
            result["product_name"] = str(result["product_name"]).strip()
            if not result["product_name"]:
                result["product_name"] = None

        # Clean up brand
        if result["brand"]:
            result["brand"] = str(result["brand"]).strip()
            if not result["brand"]:
                result["brand"] = None

        # Check if we got the essential data (at least price or MRP)
        # If we have price data, mark as success even if name is missing
        if result["product_name"] or result["mrp"] or result["price"]:
            result["success"] = True
            # Use product ID as fallback name if name is missing
            if not result["product_name"]:
                result["product_name"] = f"Product {product_id}"
        else:
            # Add methods tried to parsing details
            parsing_details["methods_tried"] = methods_tried

            # Build detailed error message
            error_parts = [
                f"Could not parse product data. Methods tried: {', '.join(methods_tried)}."
            ]

            if (
                not parsing_details["found_myx_data"]
                and not parsing_details["found_pdp_data"]
            ):
                if (
                    parsing_details["found_pdp_match"]
                    and parsing_details["pdp_json_error"]
                ):
                    error_parts.append(
                        f"Found pdpData but JSON parse failed: {parsing_details['pdp_json_error']}"
                    )
                else:
                    error_parts.append("No JSON data found in page.")

                if parsing_details["html_length"] < 5000:
                    error_parts.append(
                        f"Response too short ({parsing_details['html_length']} chars) - Myntra is blocking requests from this server/IP."
                    )
                elif not parsing_details["has_script_tags"]:
                    error_parts.append(
                        "No script tags found - Myntra may be serving a challenge page."
                    )
                else:
                    error_parts.append(
                        "Myntra may be blocking requests or page structure changed."
                    )
            elif parsing_details["found_pdp_data"]:
                error_parts.append("Found pdpData but missing 'name' field.")
                error_parts.append(
                    f"Available keys: {parsing_details.get('pdp_keys', [])}"
                )

            # Proxy disabled - using direct local IP
            error_parts.append(
                "Note: Using direct connection. Myntra may block cloud server IPs. For production, enable proxy support."
            )

            result["error"] = " ".join(error_parts)
            result["debug_info"] = parsing_details
            result["proxy_enabled"] = False
            result["proxy_type"] = None

        return result

    except requests.exceptions.Timeout:
        result["error"] = (
            f"Request timeout. Methods tried: {', '.join(methods_tried) if methods_tried else 'none'}. Myntra may be slow or blocking."
        )
        return result
    except requests.exceptions.RequestException as e:
        result["error"] = (
            f"Request error: {str(e)}. Methods tried: {', '.join(methods_tried) if methods_tried else 'none'}"
        )
        return result
    except Exception as e:
        result["error"] = (
            f"Error: {str(e)}. Methods tried: {', '.join(methods_tried) if methods_tried else 'none'}"
        )
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

        # Rate limiting settings (proxy disabled - using direct local IP)
        BATCH_SIZE = 50  # Process 50 products at a time
        DELAY_BETWEEN_BATCHES = 5  # 5 seconds delay between batches
        MAX_WORKERS = 10  # Max concurrent requests within a batch

        # ScraperAPI code commented out (proxy disabled)
        # proxy_config = get_proxy_config()
        # using_scraper_api = (
        #     proxy_config["enabled"] and proxy_config["type"] == "scraper_api"
        # )
        # if using_scraper_api:
        #     BATCH_SIZE = 10  # Smaller batches for ScraperAPI
        #     DELAY_BETWEEN_BATCHES = 2  # Less delay needed with proxy
        #     MAX_WORKERS = 3  # Lower concurrency to avoid timeouts

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
        # Support both single column (legacy) and multiple columns
        merge_columns = data.get("merge_columns", [])
        if not merge_columns:
            # Fallback to single merge_column for backwards compatibility
            single_col = data.get("merge_column")
            if single_col:
                merge_columns = [single_col]
        merge_type = data.get("merge_type", "left")  # left, right, outer, inner
        fallback_column = data.get(
            "fallback_column"
        )  # Optional fallback column from main file

        if not main_file_id:
            return JsonResponse(
                {"success": False, "error": "Main file is required"}, status=400
            )

        if not merge_columns:
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

        # Select only specified columns from main file (always include merge columns and fallback)
        if main_columns:
            columns_to_keep = list(set(main_columns + merge_columns))
            if fallback_column:
                columns_to_keep.append(fallback_column)
            columns_to_keep = list(set(columns_to_keep))
            columns_to_keep = [c for c in columns_to_keep if c in main_df.columns]
            main_df = main_df[columns_to_keep]

        # Apply renames to main file
        main_renames = data.get("main_renames", {})
        if main_renames:
            main_df = main_df.rename(columns=main_renames)
            # Update merge_columns if any were renamed
            merge_columns = [main_renames.get(c, c) for c in merge_columns]
            # Update fallback_column if renamed
            if fallback_column:
                fallback_column = main_renames.get(fallback_column, fallback_column)

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
            if child_columns:
                columns_to_keep = list(set(child_columns + merge_columns))
                columns_to_keep = [c for c in columns_to_keep if c in child_df.columns]
                child_df = child_df[columns_to_keep]

            # Apply renames to child file
            child_renames = child_info.get("renames", {})
            if child_renames:
                child_df = child_df.rename(columns=child_renames)

            # Remove rows where merge column has NA (prevents duplicate/wrong matches)
            for merge_col in merge_columns:
                if merge_col in child_df.columns:
                    child_df = child_df[child_df[merge_col].notna()]

            child_dfs.append(child_df)

        # Merge all child DataFrames first (if multiple)
        if len(child_dfs) > 1:
            merged_children = child_dfs[0]
            for i, cdf in enumerate(child_dfs[1:], start=1):
                # Handle duplicate columns by adding suffix
                merged_children = pd.merge(
                    merged_children,
                    cdf,
                    on=merge_columns,
                    how="outer",
                    suffixes=("", f"_child{i + 1}"),
                )
        elif len(child_dfs) == 1:
            merged_children = child_dfs[0]
        else:
            merged_children = None

        # Merge with main file
        if merged_children is not None:
            merge_col = merge_columns[0]  # Primary merge column

            if fallback_column and fallback_column in result_df.columns:
                # Merge with fallback logic:
                # - If merge_col has value: try to match on merge_col
                # - If merge_col is NA but fallback has value: try to match on fallback
                # - If both are NA: keep row as-is (no merge) - unless inner join

                # Get child data columns (columns from merged_children except merge columns)
                child_data_cols = [
                    c for c in merged_children.columns if c not in merge_columns
                ]

                # Initialize result with main data
                result_df = result_df.copy()

                # Add empty columns for child data
                for col in child_data_cols:
                    result_df[col] = None

                # Create lookup dict from merged_children (merge_col -> row data)
                child_lookup = {}
                for _, row in merged_children.iterrows():
                    key = row[merge_col]
                    if pd.notna(key):
                        child_lookup[key] = row[child_data_cols].to_dict()

                # Track which rows matched (for inner join)
                matched_indices = []

                # Process each row in main file
                for idx in result_df.index:
                    main_merge_val = result_df.loc[idx, merge_col]
                    main_fallback_val = (
                        result_df.loc[idx, fallback_column]
                        if fallback_column in result_df.columns
                        else None
                    )

                    matched_data = None

                    # Step 1: Try primary merge column (only if not NA)
                    if pd.notna(main_merge_val) and main_merge_val in child_lookup:
                        matched_data = child_lookup[main_merge_val]

                    # Step 2: If no match and merge_col is NA or no match, try fallback (only if fallback is not NA)
                    elif (
                        pd.notna(main_fallback_val)
                        and main_fallback_val in child_lookup
                    ):
                        matched_data = child_lookup[main_fallback_val]

                    # Apply matched data if found
                    if matched_data:
                        matched_indices.append(idx)
                        for col, val in matched_data.items():
                            result_df.loc[idx, col] = val

                # Apply join type filter
                if merge_type == "inner":
                    # Inner join: only keep rows that matched
                    result_df = result_df.loc[matched_indices]
                elif merge_type == "right":
                    # Right join: keep all child rows + matched main rows
                    # Get child keys that were matched
                    matched_child_keys = set()
                    for idx in matched_indices:
                        main_val = result_df.loc[idx, merge_col]
                        fallback_val = (
                            result_df.loc[idx, fallback_column]
                            if fallback_column in result_df.columns
                            else None
                        )
                        if pd.notna(main_val) and main_val in child_lookup:
                            matched_child_keys.add(main_val)
                        elif pd.notna(fallback_val) and fallback_val in child_lookup:
                            matched_child_keys.add(fallback_val)

                    # Keep matched main rows
                    result_df = result_df.loc[matched_indices]

                    # Add unmatched child rows
                    for key in child_lookup:
                        if key not in matched_child_keys:
                            new_row = {col: None for col in result_df.columns}
                            new_row[merge_col] = key
                            new_row.update(child_lookup[key])
                            result_df = pd.concat(
                                [result_df, pd.DataFrame([new_row])], ignore_index=True
                            )
                # For "left" and "outer" with fallback, all main rows are kept (current behavior)

            else:
                # Regular merge without fallback
                # But still avoid merging on NA values in main file
                main_with_value = result_df[result_df[merge_col].notna()]
                main_with_na = result_df[result_df[merge_col].isna()]

                # Merge only rows that have valid merge column values
                if len(main_with_value) > 0:
                    merged_part = pd.merge(
                        main_with_value,
                        merged_children,
                        on=merge_columns,
                        how=merge_type,
                        suffixes=("_main", "_child"),
                    )
                else:
                    merged_part = main_with_value.copy()
                    for col in merged_children.columns:
                        if col not in merge_columns and col not in merged_part.columns:
                            merged_part[col] = None

                # Add NA rows back only for left/outer joins (not inner/right)
                if merge_type in ["left", "outer"] and len(main_with_na) > 0:
                    # Add missing columns to NA rows
                    for col in merged_children.columns:
                        if col not in merge_columns and col not in main_with_na.columns:
                            main_with_na = main_with_na.copy()
                            main_with_na[col] = None

                    # Ensure columns match
                    for col in merged_part.columns:
                        if col not in main_with_na.columns:
                            main_with_na[col] = None

                    main_with_na = main_with_na[merged_part.columns]
                    result_df = pd.concat(
                        [merged_part, main_with_na], ignore_index=True
                    )
                else:
                    result_df = merged_part

        # Clean the result
        result_df = result_df.fillna("")

        # Convert to records for preview
        preview_data = result_df.head(100).to_dict(orient="records")
        preview_data = clean_for_json(preview_data)

        # Get column info
        result_columns = result_df.columns.tolist()

        # Generate the Excel file in memory (works on all platforms including Railway)
        import io
        import base64
        import uuid

        # Create Excel file in memory
        output = io.BytesIO()
        result_df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)

        # Encode as base64 for download
        file_base64 = base64.b64encode(output.getvalue()).decode("utf-8")
        merged_filename = f"merged_{uuid.uuid4().hex[:8]}.xlsx"

        return JsonResponse(
            {
                "success": True,
                "total_rows": len(result_df),
                "total_columns": len(result_columns),
                "columns": result_columns,
                "preview": preview_data,
                "file_data": file_base64,
                "filename": merged_filename,
            }
        )

    except Exception as e:
        import traceback

        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)}, status=400)


# =============================================================================
# Formula Columns Feature
# =============================================================================


def formula_columns(request):
    """Render the formula columns page."""
    return render(request, "aggregator/formula_columns.html")


@require_http_methods(["POST"])
def formula_upload(request):
    """Upload Excel file for formula processing."""
    try:
        if "file" not in request.FILES:
            return JsonResponse(
                {"success": False, "error": "No file provided"}, status=400
            )

        uploaded_file = request.FILES["file"]
        file_name = uploaded_file.name.lower()

        # Save the file
        excel_instance = UploadedExcel(file=uploaded_file)
        excel_instance.save()

        file_path = excel_instance.file.path

        # Read all sheets
        sheets_info = {}

        if file_name.endswith(".csv"):
            df = pd.read_csv(file_path)

            # Handle MultiIndex columns (flatten if needed)
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten MultiIndex columns by joining levels
                df.columns = [
                    "_".join(str(c) for c in col).strip() for col in df.columns.values
                ]

            # Convert columns to list and then to strings
            column_list = df.columns.tolist()

            # Filter out unnamed columns and ensure they are strings
            columns = []
            for col in column_list:
                col_str = str(col).strip()
                # Skip unnamed columns and empty strings
                if col_str and not col_str.startswith("Unnamed") and col_str != "":
                    columns.append(col_str)

            sheets_info["Sheet1"] = {
                "columns": columns,
                "numeric_columns": [
                    col
                    for col in columns
                    if col in df.select_dtypes(include=[np.number]).columns
                ],
                "row_count": len(df),
            }
        else:
            # Read Excel with all sheets
            xl = pd.ExcelFile(file_path)
            for sheet_name in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name=sheet_name)

                # Handle MultiIndex columns (flatten if needed)
                if isinstance(df.columns, pd.MultiIndex):
                    # Flatten MultiIndex columns by joining levels
                    df.columns = [
                        "_".join(str(c) for c in col).strip()
                        for col in df.columns.values
                    ]

                # Convert columns to list and then to strings
                # This ensures we get actual column names, not indices
                column_list = df.columns.tolist()

                # Filter out unnamed columns and ensure they are strings
                columns = []
                for col in column_list:
                    col_str = str(col).strip()
                    # Skip unnamed columns and empty strings
                    if col_str and not col_str.startswith("Unnamed") and col_str != "":
                        columns.append(col_str)

                sheets_info[sheet_name] = {
                    "columns": columns,
                    "numeric_columns": [
                        col
                        for col in columns
                        if col in df.select_dtypes(include=[np.number]).columns
                    ],
                    "row_count": len(df),
                }

        return JsonResponse(
            {
                "success": True,
                "file_id": excel_instance.id,
                "sheets": sheets_info,
            }
        )

    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)


@require_http_methods(["POST"])
def formula_validate(request):
    """Validate formula definitions."""
    try:
        from .formula_engine import create_formula_engine, ExcelError

        data = json.loads(request.body)
        # Support both single file_id and multiple files
        files_data = data.get("files", [])  # New: list of { fileId, label }
        file_id = data.get("file_id")  # Legacy support
        column_defs = data.get("columns", [])

        # Handle legacy single file format
        if not files_data and file_id:
            files_data = [{"fileId": file_id, "label": "File1"}]

        if not files_data:
            return JsonResponse(
                {"success": False, "error": "No files provided"}, status=400
            )

        if not column_defs:
            return JsonResponse(
                {"success": False, "error": "No column definitions provided"},
                status=400,
            )

        # Load all files into a combined dataframes dict
        dataframes = {}
        for file_info in files_data:
            fid = file_info.get("fileId")
            label = file_info.get("label", f"File{fid}")

            excel_instance = get_object_or_404(UploadedExcel, id=fid)
            file_path = excel_instance.file.path

            if file_path.lower().endswith(".csv"):
                sheet_key = f"{label}.Sheet1"
                dataframes[sheet_key] = pd.read_csv(file_path)
            else:
                xl = pd.ExcelFile(file_path)
                for sheet_name in xl.sheet_names:
                    sheet_key = f"{label}.{sheet_name}"
                    dataframes[sheet_key] = pd.read_excel(xl, sheet_name=sheet_name)

        # Create execution manager
        manager = create_formula_engine(dataframes)

        # Add column definitions
        for col_def in column_defs:
            name = col_def.get("name", "").strip()
            formula = col_def.get("formula", "").strip()
            sheet = col_def.get("sheet", list(dataframes.keys())[0])

            if not name:
                return JsonResponse(
                    {
                        "success": False,
                        "valid": False,
                        "errors": [
                            {"type": "VALUE", "message": "Column name is required"}
                        ],
                    },
                    status=400,
                )

            if not formula:
                return JsonResponse(
                    {
                        "success": False,
                        "valid": False,
                        "errors": [
                            {
                                "type": "VALUE",
                                "message": f"Formula is required for column '{name}'",
                            }
                        ],
                    },
                    status=400,
                )

            # Ensure formula starts with =
            if not formula.startswith("="):
                formula = "=" + formula

            manager.add_column_definition(name, formula, sheet)

        # Validate
        validation = manager.validate()

        return JsonResponse(
            {
                "success": True,
                "valid": validation["valid"],
                "errors": validation.get("errors", []),
                "warnings": validation.get("warnings", []),
                "dependency_order": validation.get("dependency_order", []),
            }
        )

    except ExcelError as e:
        return JsonResponse(
            {
                "success": False,
                "valid": False,
                "errors": [
                    {
                        "type": e.error_type,
                        "error_code": e.error_code,
                        "message": e.message,
                        "details": e.details,
                    }
                ],
            }
        )
    except Exception as e:
        import traceback

        traceback.print_exc()
        return JsonResponse(
            {
                "success": False,
                "valid": False,
                "errors": [{"type": "ERROR", "message": str(e)}],
            },
            status=400,
        )


@require_http_methods(["POST"])
def formula_execute(request):
    """Execute formula definitions and return results."""
    try:
        from .formula_engine import create_formula_engine, ExcelError
        import io
        import base64
        import uuid

        data = json.loads(request.body)
        # Support both single file_id and multiple files
        files_data = data.get("files", [])  # New: list of { fileId, label }
        file_id = data.get("file_id")  # Legacy support
        column_defs = data.get("columns", [])
        target_sheet = data.get("target_sheet")  # New: where to add columns

        # Handle legacy single file format
        if not files_data and file_id:
            files_data = [{"fileId": file_id, "label": "File1"}]

        if not files_data:
            return JsonResponse(
                {"success": False, "error": "No files provided"}, status=400
            )

        # Load all files into a combined dataframes dict
        # Key format: "FileLabel.SheetName"
        dataframes = {}
        default_sheet = None

        for file_info in files_data:
            fid = file_info.get("fileId")
            label = file_info.get("label", f"File{fid}")

            excel_instance = get_object_or_404(UploadedExcel, id=fid)
            file_path = excel_instance.file.path

            if file_path.lower().endswith(".csv"):
                sheet_key = f"{label}.Sheet1"
                dataframes[sheet_key] = pd.read_csv(file_path)
                if not default_sheet:
                    default_sheet = sheet_key
            else:
                xl = pd.ExcelFile(file_path)
                for sheet_name in xl.sheet_names:
                    sheet_key = f"{label}.{sheet_name}"
                    dataframes[sheet_key] = pd.read_excel(xl, sheet_name=sheet_name)
                    if not default_sheet:
                        default_sheet = sheet_key

        # Use target sheet if provided
        if target_sheet and target_sheet in dataframes:
            default_sheet = target_sheet

        # Create execution manager
        manager = create_formula_engine(dataframes, default_sheet)

        # Add column definitions
        for col_def in column_defs:
            name = col_def.get("name", "").strip()
            formula = col_def.get("formula", "").strip()
            sheet = col_def.get("sheet", default_sheet)

            if not formula.startswith("="):
                formula = "=" + formula

            manager.add_column_definition(name, formula, sheet)

        # Execute
        result = manager.execute()

        if not result["success"]:
            return JsonResponse(
                {
                    "success": False,
                    "error": "Execution failed",
                    "errors": result.get("errors", []),
                },
                status=400,
            )

        # Get result DataFrame
        result_df = manager.get_result_dataframe(default_sheet)

        # Count cell errors
        cell_errors = 0
        for col in result_df.columns:
            for val in result_df[col]:
                if isinstance(val, str) and val.startswith("#"):
                    cell_errors += 1

        # Prepare preview (first 20 rows)
        preview_df = result_df.head(20)
        preview_data = []
        for _, row in preview_df.iterrows():
            row_dict = {}
            for col in preview_df.columns:
                val = row[col]
                row_dict[col] = clean_for_json(val)
            preview_data.append(row_dict)

        # Get all columns
        result_columns = list(result_df.columns)

        # Create Excel file in memory
        output = io.BytesIO()
        result_df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)

        # Encode as base64
        file_base64 = base64.b64encode(output.getvalue()).decode("utf-8")
        result_filename = f"formula_result_{uuid.uuid4().hex[:8]}.xlsx"

        return JsonResponse(
            {
                "success": True,
                "total_rows": len(result_df),
                "total_columns": len(result_columns),
                "new_columns": len(column_defs),
                "cell_errors": cell_errors,
                "columns": result_columns,
                "preview": preview_data,
                "file_data": file_base64,
                "filename": result_filename,
            }
        )

    except ExcelError as e:
        return JsonResponse(
            {
                "success": False,
                "error": f"{e.error_code} {e.message}",
                "details": e.details,
            },
            status=400,
        )
    except Exception as e:
        import traceback

        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)}, status=400)
