import re
import importlib

from functools import cmp_to_key

def cmp_by_month_name(lhs, rhs):
    names = ["january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december"]
    length = len("COST_LIST_")
    lhs_index = names.index(lhs[length:].lower())
    rhs_index = names.index(rhs[length:].lower())
    return lhs_index - rhs_index


def split_to_lines(cost_line):
    ''' todo, no better method to extract date more than 1 line consequence'''
    # match = re.findall(r"(\d{8})", cost_list)
    # # print(match)

    # cost_line = re.split(r'\d{8}', cost_list)
    # # print(cost_line)

    # cost_line = [c.replace("\n", "") for c in cost_line if c.replace("\n", "") != ""]
    # print(cost_line)


def calc(cost_list, month_name):
    match = re.findall(r"([^\d\n]+)(\d+(?:\.\d+)?)", cost_list)
    total = 0
    for m in match:
        total += float(m[1])
    print(month_name.capitalize(), " :")
    print("Total Money : ", total)
    print("Avg Daily : ", total / len(match))
    print()
    return total, len(match)


if __name__ == "__main__":
    lib_names = ["cost_list_2019", "cost_list_2020"]
    total_money, total_month, total_days = 0, 0, 0
    for name in lib_names:
        lib = importlib.import_module(name)
        items = [item for item in dir(lib) if item.startswith("COST_LIST_")]
        items.sort(key = cmp_to_key(cmp_by_month_name))
        for item in items:
            money, days = calc(getattr(lib, item), item.split("_")[-1])
            total_days += days
            total_money += money
            total_month += 1
    print("All Total Money : ", total_money)
    print("All Avg Monthly : ", total_money / total_month)
    print("All Avg Daily : ", total_money / total_days)
