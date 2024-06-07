import os
from subprocess import Popen, PIPE

root_path = '/Users/john-y/RustroverProjects/db_simple/'
execute_file = root_path + 'target/debug/db_simple'

def run(commands, cmd_args):
    # print(cmd_args)
    commands = "\n".join(commands) + '\n'
    commands = commands.encode('utf-8')
    pipes = Popen(cmd_args, stdin=PIPE, stdout=PIPE)
    out, err = pipes.communicate(commands)
    out = str(out, encoding='utf-8')
    # print("out: ", out)
    return out

def test_insert():
    global db_file
    try:
        commands = ['insert 1 user1 person1@example.com',
                    'select',
                    '.exit']
        db_file = root_path + 'insert.db'
        cmd_args = []
        cmd_args.append(execute_file)
        cmd_args.append(db_file)
        out = run(commands, cmd_args)
        assert out == '''db > Executed.
db > 1 "user1" "person1@example.com"
Executed.
db > '''
    finally:
        os.remove(db_file)

def test_insert_too_long():
    global db_file
    try:
        commands = ['insert 1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa b',
                    '.exit']
        db_file = root_path + 'insert_too_long.db'
        cmd_args = []
        cmd_args.append(execute_file)
        cmd_args.append(db_file)
        out = run(commands, cmd_args)
        assert out == '''db > String is too long.
db > '''
    finally:
        os.remove(db_file)

def test_insert_exit_select_exit():
    global db_file
    try:
        commands = ['insert 1 user1 person1@example.com',
                    'select',
                    '.exit']
        db_file = root_path + 'insert_exit_select_exit.db'
        cmd_args = []
        cmd_args.append(execute_file)
        cmd_args.append(db_file)
        out = run(commands, cmd_args)
        # print(out)
        assert out == '''db > Executed.
db > 1 "user1" "person1@example.com"
Executed.
db > '''

        commands = ['select',
                    '.exit']
        db_file = root_path + 'insert_exit_select_exit.db'
        cmd_args = []
        cmd_args.append(execute_file)
        cmd_args.append(db_file)
        out = run(commands, cmd_args)
        assert out == '''db > 1 "user1" "person1@example.com"
Executed.
db > '''
    finally:
        os.remove(db_file)

def test_bt():
    global db_file
    try:
        commands = ['insert 1 user1 person1@example.com',
                    'insert 2 user2 person2@example.com',
                    'insert 3 user3 person3@example.com',
                    'select',
                    '.exit']
        db_file = root_path + 'bt.db'
        cmd_args = []
        cmd_args.append(execute_file)
        cmd_args.append(db_file)
        out = run(commands, cmd_args)
        # print(out)
        assert out == '''db > Executed.
db > Executed.
db > Executed.
db > 1 "user1" "person1@example.com"
2 "user2" "person2@example.com"
3 "user3" "person3@example.com"
Executed.
db > '''

        commands = ['select',
                    '.btree',
                    '.exit']
        cmd_args = []
        cmd_args.append(execute_file)
        cmd_args.append(db_file)
        out = run(commands, cmd_args)
        # print(out)
        assert out == '''db > 1 "user1" "person1@example.com"
2 "user2" "person2@example.com"
3 "user3" "person3@example.com"
Executed.
db > Tree: 
- leaf (size 3)
 1
 2
 3
db > '''
    finally:
        os.remove(db_file)

def test_bs_dup():
    global db_file
    try :
        commands = ['insert 1 user1 person1@example.com',
                    'insert 5 user5 person5@example.com',
                    'insert 2 user2 person2@example.com',
                    'select',
                    '.exit']
        db_file = root_path + 'bin_dup.db'
        cmd_args = []
        cmd_args.append(execute_file)
        cmd_args.append(db_file)
        out = run(commands, cmd_args)
        # print(out)
        assert out == '''db > Executed.
db > Executed.
db > Executed.
db > 1 "user1" "person1@example.com"
2 "user2" "person2@example.com"
5 "user5" "person5@example.com"
Executed.
db > '''

        commands = ['select',
                    'insert 1 user1 person1@example.com',
                    '.exit']
        cmd_args = []
        cmd_args.append(execute_file)
        cmd_args.append(db_file)
        out = run(commands, cmd_args)
        # print(out)
        assert out == '''db > 1 "user1" "person1@example.com"
2 "user2" "person2@example.com"
5 "user5" "person5@example.com"
Executed.
db > Error: Duplicate key.
db > '''

    finally:
        os.remove(db_file)

def test_split():
    global db_file
    try :
        commands = []
        for i in range(1, 20):
            commands.append(f"insert {i} user{i} person{i}@example.com")
        commands.append("select")
        commands.append(".exit")
        # print(commands)
        db_file = root_path + 'split.db'
        cmd_args = []
        cmd_args.append(execute_file)
        cmd_args.append(db_file)
        out = run(commands, cmd_args)
        # print(out)
        assert out == '''db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > 1 "user1" "person1@example.com"
2 "user2" "person2@example.com"
3 "user3" "person3@example.com"
4 "user4" "person4@example.com"
5 "user5" "person5@example.com"
6 "user6" "person6@example.com"
7 "user7" "person7@example.com"
8 "user8" "person8@example.com"
9 "user9" "person9@example.com"
10 "user10" "person10@example.com"
11 "user11" "person11@example.com"
12 "user12" "person12@example.com"
13 "user13" "person13@example.com"
14 "user14" "person14@example.com"
15 "user15" "person15@example.com"
16 "user16" "person16@example.com"
17 "user17" "person17@example.com"
18 "user18" "person18@example.com"
19 "user19" "person19@example.com"
Executed.
db > '''
    finally:
        os.remove(db_file)

def test_split_internal():
    global db_file
    try :
        commands = []
        for i in range(1, 100):
            commands.append(f"insert {i} user{i} person{i}@example.com")
        commands.append("select")
        commands.append(".btree")
        commands.append(".exit")
        # print(commands)
        db_file = root_path + 'split_internal.db'
        cmd_args = []
        cmd_args.append(execute_file)
        cmd_args.append(db_file)
        out = run(commands, cmd_args)
        # print(out)
        assert out == '''db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > Executed.
db > 1 "user1" "person1@example.com"
2 "user2" "person2@example.com"
3 "user3" "person3@example.com"
4 "user4" "person4@example.com"
5 "user5" "person5@example.com"
6 "user6" "person6@example.com"
7 "user7" "person7@example.com"
8 "user8" "person8@example.com"
9 "user9" "person9@example.com"
10 "user10" "person10@example.com"
11 "user11" "person11@example.com"
12 "user12" "person12@example.com"
13 "user13" "person13@example.com"
14 "user14" "person14@example.com"
15 "user15" "person15@example.com"
16 "user16" "person16@example.com"
17 "user17" "person17@example.com"
18 "user18" "person18@example.com"
19 "user19" "person19@example.com"
20 "user20" "person20@example.com"
21 "user21" "person21@example.com"
22 "user22" "person22@example.com"
23 "user23" "person23@example.com"
24 "user24" "person24@example.com"
25 "user25" "person25@example.com"
26 "user26" "person26@example.com"
27 "user27" "person27@example.com"
28 "user28" "person28@example.com"
29 "user29" "person29@example.com"
30 "user30" "person30@example.com"
31 "user31" "person31@example.com"
32 "user32" "person32@example.com"
33 "user33" "person33@example.com"
34 "user34" "person34@example.com"
35 "user35" "person35@example.com"
36 "user36" "person36@example.com"
37 "user37" "person37@example.com"
38 "user38" "person38@example.com"
39 "user39" "person39@example.com"
40 "user40" "person40@example.com"
41 "user41" "person41@example.com"
42 "user42" "person42@example.com"
43 "user43" "person43@example.com"
44 "user44" "person44@example.com"
45 "user45" "person45@example.com"
46 "user46" "person46@example.com"
47 "user47" "person47@example.com"
48 "user48" "person48@example.com"
49 "user49" "person49@example.com"
50 "user50" "person50@example.com"
51 "user51" "person51@example.com"
52 "user52" "person52@example.com"
53 "user53" "person53@example.com"
54 "user54" "person54@example.com"
55 "user55" "person55@example.com"
56 "user56" "person56@example.com"
57 "user57" "person57@example.com"
58 "user58" "person58@example.com"
59 "user59" "person59@example.com"
60 "user60" "person60@example.com"
61 "user61" "person61@example.com"
62 "user62" "person62@example.com"
63 "user63" "person63@example.com"
64 "user64" "person64@example.com"
65 "user65" "person65@example.com"
66 "user66" "person66@example.com"
67 "user67" "person67@example.com"
68 "user68" "person68@example.com"
69 "user69" "person69@example.com"
70 "user70" "person70@example.com"
71 "user71" "person71@example.com"
72 "user72" "person72@example.com"
73 "user73" "person73@example.com"
74 "user74" "person74@example.com"
75 "user75" "person75@example.com"
76 "user76" "person76@example.com"
77 "user77" "person77@example.com"
78 "user78" "person78@example.com"
79 "user79" "person79@example.com"
80 "user80" "person80@example.com"
81 "user81" "person81@example.com"
82 "user82" "person82@example.com"
83 "user83" "person83@example.com"
84 "user84" "person84@example.com"
85 "user85" "person85@example.com"
86 "user86" "person86@example.com"
87 "user87" "person87@example.com"
88 "user88" "person88@example.com"
89 "user89" "person89@example.com"
90 "user90" "person90@example.com"
91 "user91" "person91@example.com"
92 "user92" "person92@example.com"
93 "user93" "person93@example.com"
94 "user94" "person94@example.com"
95 "user95" "person95@example.com"
96 "user96" "person96@example.com"
97 "user97" "person97@example.com"
98 "user98" "person98@example.com"
99 "user99" "person99@example.com"
Executed.
db > Tree: 
- internal (size 1)
 - internal (size 1)
  - internal (size 1)
   - leaf (size 7)
    1
    2
    3
    4
    5
    6
    7
   - key 7
   - leaf (size 7)
    8
    9
    10
    11
    12
    13
    14
  - key 14
  - internal (size 1)
   - leaf (size 7)
    15
    16
    17
    18
    19
    20
    21
   - key 21
   - leaf (size 7)
    22
    23
    24
    25
    26
    27
    28
 - key 28
 - internal (size 3)
  - internal (size 1)
   - leaf (size 7)
    29
    30
    31
    32
    33
    34
    35
   - key 35
   - leaf (size 7)
    36
    37
    38
    39
    40
    41
    42
  - key 42
  - internal (size 1)
   - leaf (size 7)
    43
    44
    45
    46
    47
    48
    49
   - key 49
   - leaf (size 7)
    50
    51
    52
    53
    54
    55
    56
  - key 56
  - internal (size 1)
   - leaf (size 7)
    57
    58
    59
    60
    61
    62
    63
   - key 63
   - leaf (size 7)
    64
    65
    66
    67
    68
    69
    70
  - key 70
  - internal (size 3)
   - leaf (size 7)
    71
    72
    73
    74
    75
    76
    77
   - key 77
   - leaf (size 7)
    78
    79
    80
    81
    82
    83
    84
   - key 84
   - leaf (size 7)
    85
    86
    87
    88
    89
    90
    91
   - key 91
   - leaf (size 8)
    92
    93
    94
    95
    96
    97
    98
    99
db > '''
    finally:
        os.remove(db_file)

test_insert()
test_insert_too_long()
test_insert_exit_select_exit()
test_bt()
test_bs_dup()
test_split()
test_split_internal()

