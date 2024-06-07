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
        for i in range(1, 90):
            commands.append(f"insert {i} user{i} person{i}@example.com")
        commands.append("select")
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
db > Need to implement splitting internal node
'''
    finally:
        os.remove(db_file)

test_insert()
test_insert_too_long()
test_insert_exit_select_exit()
test_bt()
test_bs_dup()
test_split()
test_split_internal()

