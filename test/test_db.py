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
    os.remove(db_file)

def test_insert_too_long():
    commands = ['insert 1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa b',
                '.exit']
    db_file = root_path + 'insert_too_long.db'
    cmd_args = []
    cmd_args.append(execute_file)
    cmd_args.append(db_file)
    out = run(commands, cmd_args)
    assert out == '''db > String is too long.
db > '''
    os.remove(db_file)

def test_insert_exit_select_exit():
    commands = ['insert 1 user1 person1@example.com',
                'select',
                '.exit']
    db_file = root_path + 'insert_exit_select_exit.db'
    cmd_args = []
    cmd_args.append(execute_file)
    cmd_args.append(db_file)
    out = run(commands, cmd_args)
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


    os.remove(db_file)

test_insert()
test_insert_too_long()
test_insert_exit_select_exit()
