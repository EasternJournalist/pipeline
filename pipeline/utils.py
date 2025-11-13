from typing import Union, List, Dict, Any, Callable, Optional

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
    if it_per_sec < 1:
        return f"{format_time(1 / it_per_sec)}/it"
    else:
        return f"{it_per_sec:.2f} it/s"


def format_table(data: List[dict], sep: str = " | ", fill: str = "-", sort_keys=False, formatter: Optional[Union[Callable[[Any], str], Dict[str, Callable[[Any], str]]]] = str) -> str:
    if not data:
        print("(empty)")
        return
    if callable(formatter):
        formatter = {k: formatter for k in data[0].keys()}
    if sort_keys:
        keys = sorted({k for d in data for k in d})
    else:
        keys = []
        for d in data:
            for k in d.keys():
                if k not in keys:
                    keys.append(k)
    data = [{k: formatter.get(k, str)(d[k]) if k in d else fill for k in keys} for d in data]
    widths = {
        k: max(len(str(k)), *(len(row.get(k, fill)) for row in data))
        for k in keys
    }
    header = sep.join(f"{k:{widths[k]}}" for k in keys)
    lines = []
    lines.append(header)
    lines.append("-" * len(header))
    for row in data:
        line = sep.join(f"{row.get(k, fill):{widths[k]}}" for k in keys)
        lines.append(line)
    return "\n".join(lines)


def print_table(data: List[dict], sep=" | ", fill="-", sort_keys=False, formatter: Optional[Union[Callable[[Any], str], Dict[str, Callable[[Any], str]]]] = str):
    table_str = format_table(data, sep=sep, fill=fill, sort_keys=sort_keys, formatter=formatter)
    print(table_str)