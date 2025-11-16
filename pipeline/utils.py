from typing import Union, List, Dict, Any, Callable, Optional, Literal

def format_time(seconds: float) -> str:
    if seconds < 1e-3:
        return f"{seconds * 1e6:.2f} µs"
    elif seconds < 1:
        return f"{seconds * 1e3:.2f} ms"
    elif seconds < 60:
        return f"{seconds:.2f} s"
    elif seconds < 3600:
        return f"{seconds / 60:.2f} min"
    else:
        return f"{seconds / 3600:.2f} h"


def format_throughput(it_per_sec: float) -> str:
    if it_per_sec == 0:
        return "0 it/s"
    elif it_per_sec < 10 / 3600:
        return f"{it_per_sec * 3600:.2f} it / h"
    elif it_per_sec < 10 / 60:
        return f"{it_per_sec * 60:.2f} it / min"
    else:
        return f"{it_per_sec:.2f} it/s"


def _column_pad(s, width, align: str) -> str:
    if align == 'left':
        return f"{s:<{width}}"
    elif align == 'center':
        return f"{s:^{width}}"
    elif align == 'right':
        return f"{s:>{width}}"
    else:
        raise ValueError(f"Unknown align: {align}")


def format_table(
    data: List[dict], 
    sep: str = " | ", 
    fill: str = "-", 
    formatter: Optional[Union[Callable[[Any], str], Dict[str, Callable[[Any], str]]]] = str, 
    columns: Optional[List[str]] = None,
    align: Union[Literal['left', 'center', 'right'], Dict[str, str]] = "left"
) -> str:
    if not data:
        print("(empty)")
        return
    if columns is None:
        columns = []
        for d in data:
            for k in d.keys():
                if k not in columns:
                    columns.append(k)
    if callable(formatter):
        formatter = {k: formatter for k in columns}
    if isinstance(align, str):
        align = {k: align for k in columns}
    data = [{k: formatter.get(k, str)(d[k]) if k in d else fill for k in columns} for d in data]
    widths = {
        k: max(len(str(k)), *(len(row.get(k, fill)) for row in data))
        for k in columns
    }
    lines = []
    header = sep.join(_column_pad(k, widths[k], align.get(k, 'left')) for k in columns)
    lines.append(header)
    lines.append(sep.join(fill * widths[k] for k in columns))
    for row in data:
        line = sep.join(_column_pad(row.get(k, fill), widths[k], align.get(k, 'left')) for k in columns)
        lines.append(line)
    return "\n".join(lines)


def print_table(
    data: List[dict], sep=" | ", 
    fill="-", 
    formatter: Optional[Union[Callable[[Any], str], Dict[str, Callable[[Any], str]]]] = str,
    columns: Optional[List[str]] = None,
    align: Union[Literal['left', 'center', 'right'], Dict[str, str]] = "left"
) -> None:
    table_str = format_table(data, sep=sep, fill=fill, formatter=formatter, columns=columns, align=align)
    print(table_str)


def format_percent_colored(value: float, minor: float = 0.05, moderate: float = 0.25, severe: float = 0.5) -> str:
    if value >= severe:
        return f"\033[1;31m{value * 100:5.1f} %\033[0m"  # Red
    elif value >= moderate:
        return f"\033[1;33m{value * 100:5.1f} %\033[0m"  # Yellow
    elif value >= minor:
        return f"\033[1;32m{value * 100:5.1f} %\033[0m" # Blue
    else:
        # Green
        return f"\033[1;34m{value * 100:5.1f} %\033[0m"
    

def format_percent(value: float, minor: float = 0.05, significant: float = 0.25, major: float = 0.5) -> str:
    if value is None:
        return "     -  "
    elif value >= major:
        return f"{value * 100:5.1f} %"
    elif value >= significant:
        return f"{value * 100:5.1f} %"
    elif value >= minor:
        return f"{value * 100:5.1f} %"
    else:
        return f"{value * 100:5.1f} %"