use std::{io,};
use std::io::Write;
use crate::MetaCommandResult::{META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED_COMMAND};
use crate::PrepareResult::{PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT};
use crate::StatementType::{STATEMENT_INSERT, STATEMENT_SELECT};

enum ExecuteResult {
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY,
}

enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
}

enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
}

enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
}
struct InputBuffer {
    buffer : String,
}

struct Statement {
    kind: StatementType,
    row_to_insert: Row,
}

const COLUMN_USERNAME_SIZE:u32 = 32;
const COLUMN_EMAIL_SIZE:u32 = 255;

struct Row {
    id: usize,
    username: [char; (COLUMN_USERNAME_SIZE + 1) as usize],
    email: [char; (COLUMN_EMAIL_SIZE + 1) as usize],
}
const ID_SIZE:usize = 4;
const USERNAME_SIZE:usize = 32;
const EMAIL_SIZE:usize = 255;
const ROW_SIZE:usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const PAGE_SIZE:usize = 4096;
const TABLE_MAX_PAGES:usize = 100;
const ROWS_PER_PAGE:usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS:usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Table {
    num_rows: usize,
    pages:[usize; TABLE_MAX_PAGES],
}
fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn read_input() -> InputBuffer {
    let mut buffer = String::new();
    io::stdin().read_line(& mut buffer).expect("Failed to readline");
    buffer = buffer.trim().to_string();
    return InputBuffer {
        buffer : buffer,
    };
}

fn print_row(row : &Row) {
    println!("({}, {:?}, {:?})", row.id, row.username, row.email);
}

fn do_meta_command(input_buffer : InputBuffer) -> MetaCommandResult {
    if input_buffer.buffer == ".exit" {
        std::process::exit(0);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}


fn prepare_statement(input_buffer : InputBuffer, mut statement: &mut Statement) -> PrepareResult{
    if input_buffer.buffer.len() < 6 {
        return PREPARE_UNRECOGNIZED_STATEMENT;
    }

    let buffer1 = &input_buffer.buffer.clone()[0..6];
    match buffer1 {
        "insert" => {
            statement.kind = STATEMENT_INSERT;
            return PREPARE_SUCCESS;
        }
        "select" => {
            statement.kind = STATEMENT_SELECT;
            return PREPARE_SUCCESS;
        }
        _ => {}
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

fn execute_statement(statement : Statement) {
    match statement.kind {
        STATEMENT_INSERT => {
            println!("This is where we would do an insert.");
        }
        STATEMENT_SELECT => {
            println!("This is where we would do a select.")
        }

        _ => {}
    }
}

fn main() {
    loop {
        print_prompt();
        let input_buffer = read_input();
        let buffer = input_buffer.buffer.clone();
        let first_char = &input_buffer.buffer[0..1];
        if first_char.eq(".") {
            match do_meta_command(input_buffer) {
                META_COMMAND_SUCCESS => {continue;}
                META_COMMAND_UNRECOGNIZED_COMMAND => {
                    println!("Unrecognized command {}", buffer);
                    continue;
                }
            }
        }
        let mut statement = Statement { kind: StatementType::STATEMENT_INSERT, row_to_insert: Row {
            id: 0,
            username: [' '; 33],
            email: [' '; 256],
        } };
        let buffer = input_buffer.buffer.clone();
        match prepare_statement(input_buffer, &mut statement) {
            PREPARE_SUCCESS => {
            }
            PREPARE_UNRECOGNIZED_STATEMENT => {
                println!("Unrecognized keyword at start of '{}'.", buffer);
                continue;
            }
            _ => {}
        }

        execute_statement(statement);
        println!("Executed.");
    }
    // println!("{}", ib.buffer);
    // assert_eq!(ib.buffer, ".exit");
    // if (ib.buffer == ".exit") {
    //     println!("exit.");
    // } else {
    //     println!("error");
    // }

}
