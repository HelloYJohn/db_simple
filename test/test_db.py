from subprocess import Popen, PIPE

root_path = '/Users/john-y/RustroverProjects/db_simple/'
execute_file = root_path + 'target/debug/db_simple'

def run(commands):
    commands = "\n".join(commands) + '\n'
    commands = commands.encode('utf-8')
    pipes = Popen(execute_file, stdin=PIPE, stdout=PIPE)
    out, err = pipes.communicate(commands)
    out = str(out, encoding='utf-8')
    return out

def test_insert():
    commands = ['insert 1 user1 person1@example.com',
                'select',
                '.exit']
    out = run(commands)
    assert out == '''db > Executed.
db > 1 "user1" "person1@example.com"
Executed.
db > '''

def test_insert_too_long():
    commands = ['insert 1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa b',
                '.exit']
    out = run(commands)
    print(out)
    assert out == '''db > String is too long.
db > '''


test_insert()
test_insert_too_long()
