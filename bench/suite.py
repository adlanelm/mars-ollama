#!/usr/bin/env python3
"""Harder A/B suite with scripted checks, through llama-swap (production flags and sampling of each entry).
Categories: math, logic (brute-force-verified answers), code (hidden unit tests), instruct (format constraints),
tools (function calls), longctx (needles in a ~25k-token haystack). Synthetic data only.
Hard set (--set hard): competition math, search puzzles and algorithm code, where thinking longer might pay off.
MODEL@EFFORT sends reasoning_effort per request (Qwen3.8 template: low, medium, xhigh), no extra llama-swap entry needed.
Usage: bench/suite.py run OUT.jsonl MODEL[@EFFORT]... [--samples N] [--set hard] [--max-tokens N]   (resumable)
       bench/suite.py report OUT.jsonl --current M1,M2 --candidates C1,C2   -> markdown on stdout
Model code runs in a temp dir with python3 -I and a timeout (no sandbox beyond that; the tasks are plain algorithms)."""
import json, os, random, re, subprocess, sys, tempfile, time, urllib.request

URL = "http://localhost:11434"
MAX_TOKENS = 16384

# ---------- tasks ----------
ANS = "\n\nEnd your reply with a final line of the form `ANSWER: <answer>`."
CODE = "\n\nReply with the complete implementation in a single ```python code block (standard library only)."

def num(a):  # normalise an ANSWER line
    return re.sub(r"[\s*$`]", "", a).rstrip(".").lower()

TASKS = []
def task(tid, cat, prompt, **kw): TASKS.append(dict(id=tid, cat=cat, prompt=prompt, **kw))

# math: exact answers
task("m-incl-excl", "math", "How many integers from 1 to 1000 inclusive are divisible by 3 or by 5, but not by 7?" + ANS, answer="401")
task("m-weekday", "math", "What day of the week is 14 March 2031? Give the English weekday name." + ANS, answer="friday")
task("m-modpow", "math", "What are the last two digits of 7^2026? Give them as a number." + ANS, answer="49")
task("m-lattice", "math", "Count the lattice paths from (0,0) to (8,8) that use unit steps right (+1,0) or up (0,+1), never go above the "
     "line y = x (every point must satisfy y <= x), and never pass through the point (4,4)." + ANS, answer="1234")
task("m-digits", "math", "How many 5-digit positive integers have five distinct digits whose sum is 20?" + ANS, answer="1872")
task("m-tank", "math", "Pipe A fills a tank in 6 hours, pipe B fills it in 9 hours, and drain C empties a full tank in 12 hours. The tank "
     "starts empty with A, B and C all open. After 2 hours C is closed. How many hours after the start is the tank full? "
     "Give the number of hours as a decimal." + ANS, answer="4.2")
task("m-dice", "math", "Two fair six-sided dice are rolled. What is the probability that the sum is a prime number? "
     "Give it as a reduced fraction a/b." + ANS, answer="5/12")

# logic: answers verified by brute force
task("l-seating", "logic", "Anna, Bjorn, Cecilia and David sit in seats 1-4 from left to right, one per seat. Each drinks a different one of "
     "coffee, tea, juice and water.\n1. Anna sits directly left of the tea drinker.\n2. David does not drink coffee.\n"
     "3. The juice drinker sits in seat 4.\n4. Bjorn is not in seat 1 or seat 4.\n5. Cecilia drinks water.\n"
     "6. The coffee drinker sits next to Cecilia.\n7. Cecilia sits somewhere to the left of David.\n"
     "Who drinks coffee, and which seat is David in? Answer as `name, seat`." + ANS, check="seating")
task("l-knights", "logic", "On an island, knights always tell the truth and knaves always lie. A, B and C are each a knight or a knave.\n"
     "A says: \"Exactly one of the three of us is a knave.\"\nB says: \"A and C are the same type.\"\nC says: \"A is a knave.\"\n"
     "Which of them are knights? List the letters, or `none`." + ANS, check="knights")
task("l-schedule", "logic", "Seven jobs with durations in hours: A=3, B=2, C=4, D=2, E=3, F=1, G=2. Dependencies: C needs A finished; D needs A "
     "and B; E needs C; F needs D and E; G needs B. Two workers, each doing one job at a time; a job cannot be split. "
     "What is the minimum number of hours until all jobs are done?" + ANS, answer="11")
task("l-sequence", "logic", "What is the next term of the sequence 1, 11, 21, 1211, 111221, ...?" + ANS, answer="312211")

# code: hidden tests (each string is run separately; raises(E, f, *a) checks an exception)
task("c-merge", "code", "Write `merge_intervals(intervals)` taking a list of [start, end] integer pairs (start <= end, any order) and "
     "returning a new sorted list of merged [start, end] pairs. Intervals that overlap or touch (end == next start) are merged." + CODE,
     name="merge_intervals", tests=[
     "merge_intervals([]) == []", "merge_intervals([[1,3],[2,6],[8,10],[15,18]]) == [[1,6],[8,10],[15,18]]",
     "merge_intervals([[1,4],[4,5]]) == [[1,5]]", "merge_intervals([[5,6],[1,2]]) == [[1,2],[5,6]]",
     "merge_intervals([[1,10],[2,3],[4,5]]) == [[1,10]]", "merge_intervals([[-5,-1],[-1,0],[2,2]]) == [[-5,0],[2,2]]"])
task("c-duration", "code", "Write `parse_duration(s)` returning total seconds (int) for strings like \"1h30m\", \"2d\", \"45s\", \"1d2h3m4s\". "
     "The format is one or more <non-negative integer><unit> parts with units d, h, m, s, which must appear in that order, each at "
     "most once, with nothing else in the string (no spaces, signs or decimals). Raise ValueError for anything else, "
     "including the empty string." + CODE, name="parse_duration", tests=[
     "parse_duration('1h30m') == 5400", "parse_duration('2d') == 172800", "parse_duration('45s') == 45",
     "parse_duration('1d2h3m4s') == 93784", "parse_duration('0s') == 0", "parse_duration('90m') == 5400",
     "raises(ValueError, parse_duration, '')", "raises(ValueError, parse_duration, '30m1h')", "raises(ValueError, parse_duration, '1h1h')",
     "raises(ValueError, parse_duration, '5x')", "raises(ValueError, parse_duration, 'h')", "raises(ValueError, parse_duration, '1 h')",
     "raises(ValueError, parse_duration, '-1h')", "raises(ValueError, parse_duration, '1.5h')"])
task("c-lru", "code", "Write a class `LRUCache(capacity)` with `get(key)` (returns the value, or -1 if missing) and `put(key, value)`. "
     "Both count as a use. When full, put evicts the least recently used key. Capacity 0 stores nothing. Both operations O(1)." + CODE,
     name="LRUCache", tests=[
     "gets(LRUCache(2), [('p',1,1), ('p',2,2), ('g',1), ('p',3,3), ('g',2), ('p',4,4), ('g',1), ('g',3), ('g',4)]) == [1, -1, -1, 3, 4]",
     "gets(LRUCache(2), [('p',1,1), ('p',2,2), ('p',1,10), ('p',3,3), ('g',2), ('g',1)]) == [-1, 10]",
     "gets(LRUCache(0), [('p',1,1), ('g',1)]) == [-1]",
     "gets(LRUCache(2), [('p',1,1), ('p',2,2), ('g',1), ('p',3,3), ('g',1), ('g',2), ('g',3)]) == [1, 1, -1, 3]"])
task("c-roman", "code", "Write `roman_to_int(s)` for canonical uppercase Roman numerals 1-3999 (I V X L C D M, subtractive forms IV IX XL XC "
     "CD CM only). Raise ValueError for anything that is not the canonical numeral of its value, including the empty string, "
     "lowercase, IIII, VV, IC, MMMM." + CODE, name="roman_to_int", tests=[
     "roman_to_int('III') == 3", "roman_to_int('IV') == 4", "roman_to_int('IX') == 9", "roman_to_int('LVIII') == 58",
     "roman_to_int('MCMXCIV') == 1994", "roman_to_int('MMMCMXCIX') == 3999"] + [f"raises(ValueError, roman_to_int, {b!r})" for b in
     ["", "IIII", "VV", "IC", "VX", "MMMM", "iv", "IIV", "XM", "LL", "DCD"]])
task("c-topo", "code", "Write `topo_sort(deps)` where deps maps each node (str) to a list of its prerequisites. Nodes that appear only as "
     "prerequisites are nodes too. Return the lexicographically smallest order in which every node comes after all its prerequisites "
     "(always pick the smallest available node). Raise ValueError if there is a cycle." + CODE, name="topo_sort", tests=[
     "topo_sort({'b':['a'],'c':['a']}) == ['a','b','c']", "topo_sort({'c':[],'a':['c'],'b':[]}) == ['b','c','a']", "topo_sort({}) == []",
     "raises(ValueError, topo_sort, {'x':['y'],'y':['x']})", "topo_sort({'d':['b','c'],'b':['a'],'c':['a']}) == ['a','b','c','d']",
     "raises(ValueError, topo_sort, {'a':['a']})", "topo_sort({'z':['m']}) == ['m','z']"])
task("c-bisect", "code", "This function should return the first index i with a[i] >= x in the sorted list a (len(a) if there is none), but "
     "it is wrong. Fix it.\n\n```python\ndef lower_bound(a, x):\n    lo, hi = 0, len(a) - 1\n    while lo < hi:\n        mid = (lo + hi) // 2\n"
     "        if a[mid] < x:\n            lo = mid\n        else:\n            hi = mid - 1\n    return lo\n```" + CODE, name="lower_bound", tests=[
     "lower_bound([], 5) == 0", "lower_bound([1,3,5], 0) == 0", "lower_bound([1,3,5], 3) == 1", "lower_bound([1,3,5], 4) == 2",
     "lower_bound([1,3,5], 6) == 3", "lower_bound([1,2,2,2,3], 2) == 1", "lower_bound([5], 5) == 0"])
task("c-expr", "code", "Write `eval_expr(s)` that evaluates an arithmetic expression with non-negative integer literals, + - * /, parentheses, "
     "unary minus and arbitrary spaces, with the usual precedence and left associativity. Return an exact fractions.Fraction "
     "(/ is exact division). Raise ZeroDivisionError on division by zero and ValueError on any malformed input. "
     "Do not use eval or exec." + CODE, name="eval_expr", tests=[
     "eval_expr('2+3*4') == 14", "eval_expr('(2+3)*4') == 20", "eval_expr('-3--3') == 0", "eval_expr('7/2') == Fraction(7,2)",
     "eval_expr('2*-(3+1)') == -8", "eval_expr('  4 * ( 2 - 5 ) ') == -12", "eval_expr('--2') == 2", "eval_expr('1-2-3') == -4",
     "eval_expr('8/4/2') == 1", "eval_expr('2*(3+(4-1))/3') == 4", "raises(ZeroDivisionError, eval_expr, '1/0')",
     "raises(ValueError, eval_expr, '2+')", "raises(ValueError, eval_expr, '2^3')", "raises(ValueError, eval_expr, '')",
     "raises(ValueError, eval_expr, '(1+2')", "raises(ValueError, eval_expr, '1 2')"])
task("c-glob", "code", "Write `glob_match(pattern, text)` returning True if the whole text matches. `?` matches one character, `*` any "
     "sequence (including empty), `[abc]` one character from the set, `[a-c]` a range, `[!abc]` one character not in the set. "
     "Everything else is literal. Do not use fnmatch, glob or re." + CODE, name="glob_match", tests=[
     f"glob_match({p!r}, {t!r}) is {r}" for p, t, r in [("a?c","abc",True),("a?c","ac",False),("*","",True),("a*","a",True),
     ("*.py","main.py",True),("*.py","main.pyc",False),("[abc]x","bx",True),("[!abc]x","bx",False),("[!abc]x","dx",True),
     ("[a-c]1","c1",True),("[a-c]1","d1",False),("a*b*c","aXbYbZc",True),("a*b*c","aXbYbZ",False),("**","abc",True)]])

# instruction following: scripted format checks
task("i-sentences", "instruct", "Write exactly three sentences about Stockholm. Every sentence must end with an exclamation mark, and the second "
     "sentence must contain the word \"archipelago\". Output nothing else.", check="sentences")
task("i-json", "instruct", "Describe a fictional reusable water bottle as JSON with exactly these keys: \"name\" (string), \"tags\" (a list of "
     "exactly 4 lowercase strings) and \"score\" (an integer from 1 to 10). Output only the JSON object, no code fence, no other text.", check="json")
task("i-fruits", "instruct", "List five different fruits in reverse alphabetical order, one per line, formatted as `1. fruit` to `5. fruit`, "
     "all lowercase. Output nothing else.", check="fruits")
task("i-swedish", "instruct", "Svara på svenska med högst 40 ord: varför byter löven färg på hösten?", check="swedish")

# tools
TOOLS = [
    {"type": "function", "function": {"name": "get_weather", "description": "Current weather for a city",
     "parameters": {"type": "object", "properties": {"city": {"type": "string"}, "unit": {"type": "string", "enum": ["celsius", "fahrenheit"]}},
                    "required": ["city", "unit"]}}},
    {"type": "function", "function": {"name": "convert_currency", "description": "Convert an amount between currencies (ISO 4217 codes)",
     "parameters": {"type": "object", "properties": {"amount": {"type": "number"}, "from_currency": {"type": "string"},
                    "to_currency": {"type": "string"}}, "required": ["amount", "from_currency", "to_currency"]}}},
    {"type": "function", "function": {"name": "create_calendar_event", "description": "Create a calendar event",
     "parameters": {"type": "object", "properties": {"title": {"type": "string"}, "date": {"type": "string", "description": "YYYY-MM-DD"},
                    "start_time": {"type": "string", "description": "HH:MM, 24-hour"}, "duration_minutes": {"type": "integer"}},
                    "required": ["title", "date", "start_time", "duration_minutes"]}}}]
task("t-currency", "tools", "How much is 250 euros in Swedish kronor?", tools=True, check="t-currency")
task("t-parallel", "tools", "What's the weather in Oslo and in Helsinki right now? Use celsius.", tools=True, check="t-parallel")
task("t-calendar", "tools", "Book a 45-minute meeting called \"Budget review\" on 2026-11-03 at 14:30.", tools=True, check="t-calendar")
task("t-none", "tools", "What is the capital of Norway? Answer in one word.", tools=True, check="t-none")
task("t-result", "tools", [{"role": "user", "content": "What's the weather in Kiruna right now?"},
     {"role": "assistant", "content": "", "tool_calls": [{"id": "call_1", "type": "function",
      "function": {"name": "get_weather", "arguments": "{\"city\": \"Kiruna\", \"unit\": \"celsius\"}"}}]},
     {"role": "tool", "tool_call_id": "call_1", "content": "{\"temp\": -14, \"condition\": \"heavy snow\", \"wind_kmh\": 22}"}],
     tools=True, check="t-result")

# long context: synthetic haystack (~25k tokens) with distractors
def haystack(needles, seed):
    r = random.Random(seed)
    who = ["The harbour office", "A night clerk", "The depot foreman", "Inspector Lund", "The ferry crew", "A courier from Visby",
           "The archive", "Two dock workers", "The weather station", "The quartermaster"]
    did = ["logged", "rechecked", "misplaced", "recounted", "stamped", "forwarded", "questioned", "sealed", "catalogued", "repainted"]
    what = ["the spare rope inventory", "a crate of signal lamps", "the tide tables", "the east gate ledger", "a box of brass fittings",
            "the coal delivery notes", "the pilot boat schedule", "a sack of letters", "the lighthouse lens", "the customs forms"]
    when = ["before dawn", "after the storm", "on Tuesday", "during the inspection", "late in the evening", "at the change of shift"]
    paras = []
    for i in range(380):
        s = [f"{r.choice(who)} {r.choice(did)} {r.choice(what)} {r.choice(when)}." for _ in range(4)]
        if i % 37 == 5:  # distractors that look like needles
            s.append(f"The access code for vault {r.choice([2,3,4,5,6,8,9])} is {r.randint(1000,9999)}-{r.choice(['HERON','OTTER','LYNX','WREN'])}.")
        if i % 41 == 9:
            s.append(f"Shipment ledger: crate {r.choice([11,13,21,46,48,80,82])} holds {r.randint(5,90)} units.")
        paras.append(" ".join(s))
    for frac, text in needles:
        paras.insert(int(len(paras) * frac), text)
    return "\n\n".join(paras)
task("x-needle", "longctx", "Below is a long harbour log.\n\n" + haystack([(0.62, "The access code for vault 7 is 4417-KESTREL.")], 1)
     + "\n\nWhat is the access code for vault 7?" + ANS, answer="4417-kestrel")
task("x-multi", "longctx", "Below is a long harbour log.\n\n" + haystack([(0.15, "Shipment ledger: crate 12 holds 37 units."),
     (0.5, "Shipment ledger: crate 47 holds 58 units."), (0.85, "Shipment ledger: crate 81 holds 19 units.")], 2)
     + "\n\nWhat is the total number of units in crates 12, 47 and 81 together?" + ANS, answer="114")

# ---------- hard set (answers brute-forced, code tests from reference implementations) ----------
def hard(tid, cat, prompt, **kw): task(tid, cat, prompt, hard=True, **kw)

hard("hm-noadj", "math", "How many permutations p of {1, 2, ..., 9} satisfy |p(i) - i| != 1 for every i?" + ANS, answer="55005")
hard("hm-subset", "math", "How many subsets of {1, 2, ..., 20}, including the empty set, contain no two consecutive integers and have an "
     "element sum divisible by 7?" + ANS, answer="2531")
hard("hm-pick", "math", "How many points with integer coordinates lie strictly inside the triangle with vertices (0,0), (41,13) and (17,37)?"
     + ANS, answer="636")
hard("hm-hit10", "math", "A fair six-sided die is rolled repeatedly and the results are added up. You stop as soon as the running total is 10 "
     "or more. What is the probability that you stop with a total of exactly 10? Give it as a reduced fraction a/b." + ANS,
     answer="17492167/60466176")
hard("hm-digit11", "math", "How many integers n with 1 <= n <= 1000000 have digit sum 27 and are divisible by 11?" + ANS, answer="4050")
hard("hl-jugs", "logic", "You have three jugs holding 14, 9 and 5 litres. The 14-litre jug is full of water, the others are empty. A pour "
     "moves water from one jug into another until the source is empty or the target is full; no water is added or spilled. "
     "What is the minimum number of pours until some jug holds exactly 7 litres?" + ANS, answer="12")
hard("hl-maze", "logic", "In this grid, `#` is a wall. You start at S and want to reach G, moving one cell up, down, left or right per step. "
     "A lowercase letter is a key, picked up by stepping on it; the matching uppercase letter is a door that can only be entered once "
     "you hold its key. Keys stay with you.\n\n```\n###########\n#S..#....b#\n#.#.#.###.#\n#.#...#...#\n#.#####.#A#\n#a..#...#.#\n"
     "###.#.###.#\n#...#.#..G#\n#.###.#B###\n#.........#\n###########\n```\n\nWhat is the minimum number of steps from S to G?"
     + ANS, answer="26")
hard("hl-zebra", "logic", "Five houses stand in a row, numbered 1 to 5 from left to right. Each has a different owner (Alva, Birk, Cleo, Dag, "
     "Edda), pet (cat, dog, fox, owl, hare), drink (tea, milk, cider, cocoa, water) and job (baker, pilot, nurse, smith, tailor).\n"
     "1. The cat lives next to the hare.\n2. The smith lives in house 5.\n3. Birk is the baker.\n4. Edda drinks cocoa.\n"
     "5. The milk drinker lives next to the owl.\n6. The pilot lives somewhere left of Dag.\n7. The water drinker does not keep the hare.\n"
     "8. The dog belongs to the nurse.\n9. Edda keeps the fox.\n10. The water drinker lives next to the dog.\n"
     "11. The tea drinker lives somewhere left of the milk drinker.\n12. The pilot lives somewhere left of Birk.\n"
     "13. The pilot lives directly left of the water drinker.\n14. Alva lives directly left of Edda.\n"
     "Give the owners of houses 1 to 5 in order, then the nurse's drink, as a comma-separated list." + ANS,
     answer="cleo,birk,dag,alva,edda,milk")
hard("hc-semver", "code", "Write `semver_compare(a, b)` for Semantic Versioning 2.0.0 strings, returning -1, 0 or 1 by precedence. Follow the "
     "spec exactly: MAJOR.MINOR.PATCH numeric without leading zeros, optional -pre-release and +build metadata made of dot-separated non-empty "
     "[0-9A-Za-z-] identifiers, numeric pre-release identifiers without leading zeros, build metadata ignored for precedence. Raise "
     "ValueError if either string is not a valid version." + CODE, name="semver_compare", tests=["semver_compare('1.0.0', '2.0.0') == -1", "semver_compare('2.0.0', '2.1.0') == -1", "semver_compare('2.1.1', '2.1.0') == 1", "semver_compare('1.0.0-alpha', '1.0.0') == -1", "semver_compare('1.0.0-alpha', '1.0.0-alpha.1') == -1", "semver_compare('1.0.0-alpha.1', '1.0.0-alpha.beta') == -1", "semver_compare('1.0.0-alpha.beta', '1.0.0-beta') == -1", "semver_compare('1.0.0-beta', '1.0.0-beta.2') == -1", "semver_compare('1.0.0-beta.2', '1.0.0-beta.11') == -1", "semver_compare('1.0.0-beta.11', '1.0.0-rc.1') == -1", "semver_compare('1.0.0-rc.1', '1.0.0') == -1", "semver_compare('1.0.0+build.5', '1.0.0+other') == 0", "semver_compare('1.0.0-rc.1+b', '1.0.0-rc.1') == 0", "semver_compare('10.2.3', '9.20.30') == 1", "semver_compare('1.0.0-x-y.7', '1.0.0-x-y.07a') == -1", "semver_compare('1.0.0-Alpha', '1.0.0-alpha') == -1", "semver_compare('1.0.0-0.3.7', '1.0.0-0.3.7.0') == -1", "semver_compare('0.0.0', '0.0.0') == 0", "semver_compare('1.0.0+001', '1.0.0') == 0", "semver_compare('2.0.0', '1.0.0') == 1", "semver_compare('2.1.0', '2.0.0') == 1", "semver_compare('2.1.0', '2.1.1') == -1", "semver_compare('1.0.0', '1.0.0-alpha') == 1", "semver_compare('1.0.0-alpha.1', '1.0.0-alpha') == 1", "semver_compare('1.0.0-alpha.beta', '1.0.0-alpha.1') == 1", "raises(ValueError, semver_compare, '', '1.0.0')", "raises(ValueError, semver_compare, '1.0', '1.0.0')", "raises(ValueError, semver_compare, '1.0.0.0', '1.0.0')", "raises(ValueError, semver_compare, '01.0.0', '1.0.0')", "raises(ValueError, semver_compare, '1.00.0', '1.0.0')", "raises(ValueError, semver_compare, '1.0.0-', '1.0.0')", "raises(ValueError, semver_compare, '1.0.0+', '1.0.0')", "raises(ValueError, semver_compare, '1.0.0-01', '1.0.0')", "raises(ValueError, semver_compare, '1.0.0-a..b', '1.0.0')", "raises(ValueError, semver_compare, 'v1.0.0', '1.0.0')", "raises(ValueError, semver_compare, '1.0.0-a_b', '1.0.0')", "raises(ValueError, semver_compare, ' 1.0.0', '1.0.0')", "raises(ValueError, semver_compare, '1.0.0+a..b', '1.0.0')"])
hard("hc-regex", "code", "Write `regex_match(pattern, text)` returning True if the whole text matches the pattern. Patterns contain literal "
     "characters, `.` (any character), `*` (zero or more), `+` (one or more), `?` (optional), `|` (alternation, lowest precedence) and "
     "parentheses for grouping; `*`, `+` and `?` apply to the preceding character, `.` or group. Patterns are always valid. "
     "Do not use the re module." + CODE, name="regex_match", tests=["regex_match('a*b', 'aaab') is True", "regex_match('a*b', 'b') is True", "regex_match('a*b', 'aaba') is False", "regex_match('(ab)+', 'ababab') is True", "regex_match('(ab)+', '') is False", "regex_match('(ab)+', 'aba') is False", "regex_match('a?b?c', 'c') is True", "regex_match('a?b?c', 'bc') is True", "regex_match('a?b?c', 'abbc') is False", "regex_match('(a|bc)*d', 'abcad') is True", "regex_match('(a|bc)*d', 'd') is True", "regex_match('(a|bc)*d', 'abd') is False", "regex_match('.*x.*', 'abxcd') is True", "regex_match('.*x.*', 'x') is True", "regex_match('.*x.*', 'abcd') is False", "regex_match('(a*)*b', 'aaaab') is True", "regex_match('(a*)*b', 'b') is True", "regex_match('(a*)*b', 'aaaa') is False", "regex_match('a(b|c|d)?e', 'ae') is True", "regex_match('a(b|c|d)?e', 'ade') is True", "regex_match('a(b|c|d)?e', 'abce') is False", "regex_match('((ab|a)c)+', 'acabcac') is True", "regex_match('((ab|a)c)+', 'ac') is True", "regex_match('((ab|a)c)+', 'abac') is False", "regex_match('(a|b)*abb', 'babaabb') is True", "regex_match('(a|b)*abb', 'abb') is True", "regex_match('(a|b)*abb', 'babab') is False", "regex_match('x(y(z|w)+)?', 'x') is True", "regex_match('x(y(z|w)+)?', 'xyzwzw') is True", "regex_match('x(y(z|w)+)?', 'xy') is False", "regex_match('(.)+', 'q') is True", "regex_match('(.)+', 'qq') is True", "regex_match('(.)+', '') is False", "regex_match('a+a+a', 'aaa') is True", "regex_match('a+a+a', 'aaaa') is True", "regex_match('a+a+a', 'aa') is False"])
hard("hc-mintx", "code", "Write `min_transfers(debts)` where debts is a list of (debtor, creditor, amount) tuples with names as strings and "
     "positive integer amounts. Return the minimum number of payments (each from one person to another, any amount) that settles "
     "everyone's net balance. At most 12 people. It must run within a few seconds." + CODE, name="min_transfers", tests=['min_transfers([]) == 0', "min_transfers([('A', 'B', 10)]) == 1", "min_transfers([('A', 'B', 10), ('B', 'A', 10)]) == 0", "min_transfers([('A', 'B', 5), ('B', 'C', 5)]) == 1", "min_transfers([('A', 'B', 10), ('C', 'A', 1), ('C', 'B', 5), ('D', 'A', 5)]) == 3", "min_transfers([('B', 'E', 12), ('E', 'D', 21), ('E', 'A', 20), ('A', 'D', 9), ('E', 'B', 7), ('D', 'E', 13)]) == 3", "min_transfers([('F', 'B', 8), ('F', 'B', 28), ('E', 'D', 24), ('A', 'F', 25), ('A', 'B', 25), ('E', 'A', 10), ('G', 'A', 27), ('G', 'C', 16), ('E', 'F', 30)]) == 5", "min_transfers([('G', 'F', 26), ('G', 'D', 24), ('H', 'B', 29), ('F', 'A', 2), ('C', 'D', 7), ('E', 'F', 14), ('E', 'D', 17), ('G', 'E', 12), ('G', 'E', 8), ('F', 'H', 30), ('A', 'G', 9), ('C', 'F', 28)]) == 7", "min_transfers([('F', 'B', 23), ('D', 'E', 10), ('B', 'I', 16), ('H', 'B', 12), ('B', 'G', 29), ('C', 'A', 10), ('G', 'I', 28), ('B', 'A', 20), ('A', 'G', 23), ('F', 'E', 17), ('D', 'A', 10), ('A', 'B', 4), ('I', 'A', 7), ('G', 'E', 20)]) == 7", "min_transfers([('E', 'C', 23), ('A', 'F', 11), ('F', 'C', 29), ('G', 'J', 15), ('I', 'G', 21), ('J', 'I', 4), ('J', 'I', 9), ('G', 'D', 30), ('E', 'G', 9), ('I', 'E', 18), ('F', 'A', 26), ('G', 'F', 1), ('G', 'C', 2), ('F', 'H', 12), ('F', 'E', 24), ('H', 'A', 19)]) == 7", "min_transfers([('A', 'X', 7), ('B', 'X', 3), ('C', 'Y', 6), ('D', 'Y', 4), ('E', 'Z', 5), ('F', 'Z', 5)]) == 6"])
hard("hc-kperm", "code", "Write `kth_perm(s, k)` returning the k-th (0-based) string in the sorted list of distinct permutations of the "
     "characters of s (s may repeat characters; the empty string has one permutation, itself). Raise IndexError if k is out of range. "
     "It must be fast for strings of up to 30 characters with k up to 10**13." + CODE, name="kth_perm", tests=["kth_perm('abc', 0) == 'abc'", "kth_perm('abc', 5) == 'cba'", "kth_perm('aab', 1) == 'aba'", "kth_perm('aabb', 3) == 'baab'", "kth_perm('bca', 2) == 'bac'", "kth_perm('zzzz', 0) == 'zzzz'", "kth_perm('', 0) == ''", "kth_perm('aabbc', 17) == 'bacba'", "kth_perm('mississ', 100) == 'sssmisi'", "kth_perm('aaabbbcccddd', 123456) == 'bbbccadcdada'", "kth_perm('abcdefghijklmnop', 1000000000000) == 'amhjlcdpfgoeknbi'", "kth_perm('aaaaaaaaaabbbbbbbbbbcccccccccc', 5000000000000) == 'ccaabaaccabacbcaccbcaabbbabcbb'", "raises(IndexError, kth_perm, 'aab', 3)", "raises(IndexError, kth_perm, 'abc', -1)", "raises(IndexError, kth_perm, '', 1)"])

# ---------- checks ----------
def answer_line(text):
    m = re.findall(r"ANSWER:\s*(.+)", text or "", re.I)
    return m[-1].strip() if m else None

def run_code(t, content):
    blocks = re.findall(r"```(?:python|py)?\s*\n(.*?)```", content or "", re.S)
    blocks = [b for b in blocks if t["name"] in b]
    if not blocks:
        return 0.0, "no code block"
    harness = ("import json, sys\nfrom fractions import Fraction\n"
               "def raises(e, f, *a):\n    try: f(*a)\n    except e: return True\n    except Exception: return False\n    return False\n"
               "def gets(c, ops):\n    return [c.get(o[1]) for o in ops if o[0] == 'g' or c.put(o[1], o[2])]\n"
               "src = open(sys.argv[1]).read()\ng = {'__name__': 'solution'}\nexec(compile(src, 'solution.py', 'exec'), g)\n"
               "g['raises'] = raises; g['gets'] = gets; g['Fraction'] = Fraction\nres = []\nfor t in json.loads(sys.argv[2]):\n"
               "    try: res.append(bool(eval(t, g)))\n    except Exception: res.append(False)\nprint(json.dumps(res))\n")
    with tempfile.TemporaryDirectory() as d:
        open(os.path.join(d, "solution.py"), "w").write(blocks[-1])
        open(os.path.join(d, "harness.py"), "w").write(harness)
        try:
            p = subprocess.run([sys.executable, "-I", "harness.py", "solution.py", json.dumps(t["tests"])], cwd=d,
                               capture_output=True, text=True, timeout=20)
            res = json.loads(p.stdout.strip().splitlines()[-1])
        except Exception as e:
            return 0.0, f"harness: {type(e).__name__}"
    return sum(res) / len(res), f"{sum(res)}/{len(res)} tests"

def calls(msg):
    out = []
    for c in msg.get("tool_calls") or []:
        try:
            out.append((c["function"]["name"], json.loads(c["function"]["arguments"] or "{}")))
        except Exception:
            out.append((c["function"].get("name"), None))
    return out

def check(t, msg):
    content = msg.get("content") or ""
    if "answer" in t:
        a = answer_line(content)
        return (1.0, "ok") if a and num(a) == t["answer"] else (0.0, f"answer {a!r}")
    if t["cat"] == "code":
        return run_code(t, content)
    k = t.get("check")
    if k == "seating":
        a = (answer_line(content) or "").lower()
        return (1.0, "ok") if "anna" in a and re.search(r"\b4\b", a) and not re.search(r"bjorn|cecilia|david", a) else (0.0, f"answer {a!r}")
    if k == "knights":
        a = answer_line(content) or ""
        letters = set(re.findall(r"\b([ABC])\b", a.upper()))
        return (1.0, "ok") if letters == {"C"} else (0.0, f"answer {a!r}")
    if k == "sentences":
        text = " ".join(content.split())
        parts = re.findall(r"[^.!?]+[.!?]", text)
        ok = len(parts) == 3 and "".join(parts).strip() == text and all(p.endswith("!") for p in parts) and "archipelago" in parts[1].lower()
        return (1.0 if ok else 0.0), f"{len(parts)} sentences"
    if k == "json":
        try:
            j = json.loads(content.strip())
            ok = (set(j) == {"name", "tags", "score"} and isinstance(j["name"], str) and isinstance(j["tags"], list) and len(j["tags"]) == 4
                  and all(isinstance(x, str) and x == x.lower() for x in j["tags"]) and type(j["score"]) is int and 1 <= j["score"] <= 10)
            return (1.0 if ok else 0.0), "schema" if not ok else "ok"
        except Exception:
            return 0.0, "not json"
    if k == "fruits":
        lines = [l.strip() for l in content.strip().splitlines()]
        m = [re.fullmatch(r"(\d)\. ([a-z][a-z ]*)", l) for l in lines]
        ok = len(lines) == 5 and all(m) and [x.group(1) for x in m] == list("12345")
        fr = [x.group(2) for x in m] if ok else []
        ok = ok and fr == sorted(fr, reverse=True) and len(set(fr)) == 5
        return (1.0 if ok else 0.0), "ok" if ok else f"{len(lines)} lines"
    if k == "swedish":
        words = re.findall(r"\w+", content.lower())
        sv = sum(w in {"och", "att", "är", "på", "det", "som", "en", "av", "när", "de", "i", "blir"} for w in words)
        ok = 0 < len(words) <= 40 and sv >= 3 and "the" not in words
        return (1.0 if ok else 0.0), f"{len(words)} words"
    cs = calls(msg)
    if k == "t-currency":
        ok = len(cs) == 1 and cs[0][0] == "convert_currency" and cs[0][1] and float(cs[0][1].get("amount", 0)) == 250 \
             and str(cs[0][1].get("from_currency", "")).upper() == "EUR" and str(cs[0][1].get("to_currency", "")).upper() == "SEK"
        return (1.0 if ok else 0.0), str(cs)[:120]
    if k == "t-parallel":
        ok = len(cs) == 2 and all(n == "get_weather" and a and a.get("unit") == "celsius" for n, a in cs) \
             and {a["city"].lower() for n, a in cs} == {"oslo", "helsinki"}
        return (1.0 if ok else 0.0), str(cs)[:120]
    if k == "t-calendar":
        want = {"title": "Budget review", "date": "2026-11-03", "start_time": "14:30", "duration_minutes": 45}
        ok = len(cs) == 1 and cs[0][0] == "create_calendar_event" and cs[0][1] == want
        return (1.0 if ok else 0.0), str(cs)[:120]
    if k == "t-none":
        ok = not cs and "oslo" in content.lower()
        return (1.0 if ok else 0.0), "tool called" if cs else "ok"
    if k == "t-result":
        ok = not cs and re.search(r"[-−]\s?14", content) and "snow" in content.lower()
        return (1.0 if ok else 0.0), "ok" if ok else content[:80]
    raise ValueError(k)

# ---------- run ----------
def post(path, body, timeout=1800):
    req = urllib.request.Request(URL + path, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def run(out, models, samples, hard=False, max_tokens=MAX_TOKENS):
    done = set()
    if os.path.exists(out):
        done = {(r["model"], r["task"], r["sample"]) for r in map(json.loads, open(out))}
    tasks = [t for t in TASKS if bool(t.get("hard")) == hard]
    for s in range(samples):  # samples outermost: a partial run still covers every model evenly
        for m in models:
            base, _, effort = m.partition("@")
            for t in tasks:
                if (m, t["id"], s) in done:
                    continue
                msgs = t["prompt"] if isinstance(t["prompt"], list) else [{"role": "user", "content": t["prompt"]}]
                body = {"model": base, "messages": msgs, "max_tokens": max_tokens, "cache_prompt": False}
                if effort:
                    body["reasoning_effort"] = effort
                if t.get("tools"):
                    body["tools"] = TOOLS
                t0 = time.time()
                try:
                    j = post("/v1/chat/completions", body)
                    msg, err = j["choices"][0]["message"], None
                except Exception as e:
                    j, msg, err = {}, {}, f"{type(e).__name__}: {e}"[:200]
                wall = time.time() - t0
                think = 0
                if msg.get("reasoning_content"):
                    try:
                        think = len(post(f"/upstream/{base}/tokenize", {"content": msg["reasoning_content"]})["tokens"])
                    except Exception:
                        think = -1
                score, note = check(t, msg) if msg else (0.0, err)
                rec = dict(model=m, task=t["id"], cat=t["cat"], sample=s, score=score, note=note, think=think,
                           n=j.get("usage", {}).get("completion_tokens", 0), finish=(j.get("choices") or [{}])[0].get("finish_reason"),
                           wall=round(wall, 1), tg=round(j.get("timings", {}).get("predicted_per_second", 0), 1),
                           content=msg.get("content"), tool_calls=msg.get("tool_calls"))
                with open(out, "a") as f:
                    f.write(json.dumps(rec) + "\n")
                print(f"{m:22} {t['id']:12} s{s} score={score:.2f} think={think:5} wall={wall:6.1f}s {note}", flush=True)

# ---------- report ----------
def report(out, current, candidates):
    rows = [json.loads(l) for l in open(out)]
    cats = list(dict.fromkeys(t["cat"] for t in TASKS if any(r["task"] == t["id"] for r in rows)))
    def stats(m):
        R = [r for r in rows if r["model"] == m]
        if not R:
            return None
        per = {c: sum(r["score"] for r in R if r["cat"] == c) / max(1, sum(r["cat"] == c for r in R)) for c in cats}
        tg = sorted(r["tg"] for r in R if r["tg"])
        return dict(per=per, acc=sum(r["score"] for r in R) / len(R), think=sum(max(r["think"], 0) for r in R) / len(R),
                    wall=sum(r["wall"] for r in R) / len(R), tg=tg[len(tg) // 2] if tg else 0, trunc=sum(r["finish"] == "length" for r in R), n=len(R))
    S = {m: stats(m) for m in current + candidates}
    pct = lambda x: f"{100 * x:.0f}%"
    print("| model | " + " | ".join(cats) + " | overall | think tok/task | s/task | tok/s | truncated |")
    print("|---" * (len(cats) + 6) + "|")
    for m in current + candidates:
        s = S[m]
        if s:
            print(f"| {m}{' (candidate)' if m in candidates else ''} | " + " | ".join(pct(s["per"][c]) for c in cats)
                  + f" | **{pct(s['acc'])}** | {s['think']:.0f} | {s['wall']:.1f} | {s['tg']:.1f} | {s['trunc']}/{s['n']} |")
    print("\nClosest current model = smallest mean absolute difference of the per-category scores.\n")
    print("| candidate | closest current | distance | overall | think tok/task | s/task | tok/s |")
    print("|---|---|---|---|---|---|---|")
    for c in candidates:
        if not S[c]:
            continue
        d = {m: sum(abs(S[c]["per"][k] - S[m]["per"][k]) for k in cats) / len(cats) for m in current if S[m]}
        b = min(d, key=d.get)
        f = lambda key, fmt: f"{fmt(S[c][key])} vs {fmt(S[b][key])}"
        print(f"| {c} | {b} | {d[b]:.2f} | {f('acc', pct)} | {f('think', lambda x: f'{x:.0f}')} | {f('wall', lambda x: f'{x:.1f}')} | {f('tg', lambda x: f'{x:.1f}')} |")

if __name__ == "__main__":
    a = sys.argv[1:]
    if a[:1] == ["run"]:
        n = int(a[a.index("--samples") + 1]) if "--samples" in a else 2
        mt = int(a[a.index("--max-tokens") + 1]) if "--max-tokens" in a else MAX_TOKENS
        ms = [x for i, x in enumerate(a[2:], 2) if not x.startswith("--") and a[i - 1] not in ("--samples", "--max-tokens", "--set")]
        run(a[1], ms, n, "--set" in a and a[a.index("--set") + 1] == "hard", mt)
    elif a[:1] == ["report"]:
        report(a[1], a[a.index("--current") + 1].split(","), a[a.index("--candidates") + 1].split(","))
    else:
        sys.exit(__doc__)

