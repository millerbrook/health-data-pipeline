"""Utility to parse Quest Diagnostics lab reports from PDF format."""
import re
from pathlib import Path
from datetime import datetime
import pdfplumber
import pandas as pd


def extract_date_from_filename(filename):
    """Extract test date from filename like 'labreport_1_10_2026.pdf'."""
    name = Path(filename).stem
    parts = name.split('_')
    
    if len(parts) >= 4:
        try:
            month = int(parts[1])
            day = int(parts[2])
            year = int(parts[3])
            
            # Handle 2-digit years
            if year < 100:
                year = 2000 + year if year < 50 else 1900 + year
            
            return datetime(year, month, day).date()
        except (ValueError, IndexError):
            pass
    
    # Fallback: use file modification time
    return datetime.fromtimestamp(Path(filename).stat().st_mtime).date()


def parse_reference_range(ref_text):
    """Parse reference range text into low/high values."""
    if not ref_text or pd.isna(ref_text):
        return None, None
    
    ref_text = str(ref_text).strip()
    
    # Pattern for ranges: "65-99", "3.8-10.8"
    range_match = re.search(r'(\d+\.?\d*)\s*-\s*(\d+\.?\d*)', ref_text)
    if range_match:
        return float(range_match.group(1)), float(range_match.group(2))
    
    # Pattern for "> OR = X" or ">X"
    gt_match = re.search(r'>\s*(?:OR\s*=\s*)?(\d+\.?\d*)', ref_text)
    if gt_match:
        return float(gt_match.group(1)), None
    
    # Pattern for "< X" or "<= X"
    lt_match = re.search(r'<\s*=?\s*(\d+\.?\d*)', ref_text)
    if lt_match:
        return None, float(lt_match.group(1))
    
    return None, None


def extract_value_and_flag(value_text):
    """Extract numeric value and flag (H/L) from result text."""
    if not value_text or pd.isna(value_text):
        return None, None, None
    
    value_text = str(value_text).strip()
    
    # Check for flag at end (H, L)
    flag_match = re.search(r'(H|L)\s*$', value_text)
    flag = flag_match.group(1) if flag_match else None
    
    # Remove flag for number extraction
    clean_text = re.sub(r'\s*(H|L)\s*$', '', value_text)
    
    # Try to extract numeric value
    num_match = re.search(r'^(\d+\.?\d*)$', clean_text.strip())
    if num_match:
        return float(num_match.group(1)), flag, value_text
    
    # Non-numeric result (NEGATIVE, YELLOW, etc.)
    return None, flag, value_text


def parse_lab_pdf(pdf_path):
    """Parse a Quest Diagnostics lab report PDF."""
    pdf_path = Path(pdf_path)
    test_date = extract_date_from_filename(pdf_path)
    
    results = []
    current_panel = None
    
    # Known panel header patterns
    PANEL_HEADERS = [
        'LIPID PANEL', 'COMPREHENSIVE METABOLIC PANEL', 
        'CBC (INCLUDES DIFF/PLT)', 'TSH+FREE T4', 'TSH W/REFLEX',
        'HEMOGLOBIN A1C', 'INSULIN', 'ZINC', 
        'VITAMIN B12/FOLATE', 'URINALYSIS', 'HIV', 
        'MEASLES', 'MUMPS', 'RUBELLA', 'MMR'
    ]
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            
            lines = text.split('\n')
            
            for i, line in enumerate(lines):
                line = line.strip()
                
                # Detect panel headers
                is_panel = any(panel in line.upper() for panel in PANEL_HEADERS)
                if is_panel and len(line) > 5:
                    current_panel = line
                    continue
                
                # Skip header lines, patient info, and explanatory text
                if (not line or 
                    line.startswith('Analyte') or 
                    line.startswith('MILLER,CHRISTOPHER') or
                    line.startswith('DOB:') or
                    line.startswith('Value') or
                    line.startswith('Report') or
                    line.startswith('Sex:') or
                    line.startswith('Phone:') or
                    'http' in line.lower() or
                    'guidelines' in line.lower() or
                    'control in' in line.lower() or
                    'standards of' in line.lower() or
                    len(line) < 10):
                    continue
                
                # Parse test result lines that contain "Reference Range:"
                if 'Reference Range:' not in line:
                    continue
                
                # Split by "Reference Range:" to separate test info from range
                parts = line.split('Reference Range:')
                if len(parts) != 2:
                    continue
                
                test_info = parts[0].strip()
                ref_info = parts[1].strip()
                
                # More robust regex: match number (possibly with decimal) followed by optional flag
                # Must be at the very end of test_info
                value_pattern = r'(\d+\.?\d*)\s*([HL]?)\s*$'
                value_match = re.search(value_pattern, test_info)
                
                if value_match:
                    # Found numeric value
                    numeric_value = float(value_match.group(1))
                    flag = value_match.group(2) if value_match.group(2) else None
                    result_value = numeric_value
                    result_flag = flag
                    result_text = str(numeric_value) + (f" {flag}" if flag else "")
                    
                    # Remove value and flag from test_info to get test name
                    test_name = re.sub(value_pattern, '', test_info).strip()
                else:
                    # Non-numeric result (NEGATIVE, YELLOW, SEE NOTE, etc.)
                    # Look for H/L flags even in text results
                    flag_match = re.search(r'([HL])\s*$', test_info)
                    if flag_match:
                        flag = flag_match.group(1)
                        test_name = re.sub(r'\s*[HL]\s*$', '', test_info).strip()
                        result_value = None
                        result_flag = flag
                        result_text = None
                    else:
                        # Extract potential text result (NEGATIVE, YELLOW, etc.)
                        # Last word might be the result
                        words = test_info.split()
                        if len(words) > 1:
                            last_word = words[-1]
                            # Check if last word looks like a result (all caps, not a test name)
                            if last_word.isupper() and len(last_word) < 20:
                                result_text = last_word
                                test_name = ' '.join(words[:-1])
                                result_value = None
                                result_flag = None
                            else:
                                # Couldn't parse, skip
                                continue
                        else:
                            continue  # Skip lines we can't parse
                
                # Parse reference range
                range_low, range_high = parse_reference_range(ref_info)
                
                # Extract unit from reference range text
                unit_match = re.search(r'(mg/dL|g/dL|mmol/L|mIU/L|uIU/mL|pg/mL|ng/dL|ng/mL|mcg/dL|U/L|%|Thousand/uL|Million/uL|cells/uL|fL|pg|AU/mL|Index)', 
                                      ref_info)
                result_unit = unit_match.group(1) if unit_match else None
                
                # Calculate is_abnormal
                is_abnormal = result_flag in ['H', 'L'] if result_flag else False
                
                # Skip tests with empty or very short names
                if not test_name or len(test_name) < 3:
                    continue
                
                results.append({
                    'test_date': test_date,
                    'panel_name': current_panel or 'Uncategorized',
                    'test_name': test_name,
                    'result_value': result_value,
                    'result_text': result_text,
                    'result_flag': result_flag,
                    'result_unit': result_unit,
                    'reference_range_text': ref_info,
                    'reference_range_low': range_low,
                    'reference_range_high': range_high,
                    'is_abnormal': is_abnormal,
                })
    
    return pd.DataFrame(results)


def parse_all_lab_pdfs(lab_dir):
    """Parse all PDFs in the lab results directory."""
    lab_dir = Path(lab_dir)
    all_results = []
    
    for pdf_file in sorted(lab_dir.glob('*.pdf')):
        print(f"Processing {pdf_file.name}...")
        try:
            df = parse_lab_pdf(pdf_file)
            all_results.append(df)
            print(f"  Extracted {len(df)} test results")
        except Exception as e:
            print(f"  Error parsing {pdf_file.name}: {e}")
            import traceback
            traceback.print_exc()
    
    if all_results:
        return pd.concat(all_results, ignore_index=True)
    else:
        return pd.DataFrame()