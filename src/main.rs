use std::{env, io, mem, process};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::exit;

use crate::ExecuteResult::{ExecuteDuplicateKey, ExecuteSuccess, ExecuteTableFull};
use crate::MetaCommandResult::{MetaCommandSuccess, MetaCommandUnrecognizedCommand};
use crate::NodeType::{NodeInternal, NodeLeaf};
use crate::PrepareResult::{PrepareNegativeId, PrepareStringTooLong, PrepareSuccess, PrepareSyntaxError, PrepareUnrecognizedStatement};
use crate::StatementType::{StatementInsert, StatementNone, StatementSelect};

///String -> [u8;_]
#[macro_export]
macro_rules! string_to_array {
    ($string:expr, $array_length: expr) => {{
        let mut array = [0u8; $array_length];

        for (&x, p) in $string.as_bytes().iter().zip(array.iter_mut()) {
            *p = x;
        }
        array
    }};
}

// 序列化结构体
pub unsafe fn serialize_struct<T: Sized>(src: &T) -> &[u8] {
    ::std::slice::from_raw_parts((src as *const T) as *const u8, ::std::mem::size_of::<T>())
}

// 反序列化结构体
pub unsafe fn deserialize_struct<T: Sized>(src: Vec<u8>) -> T {
    ::std::ptr::read(src.as_ptr() as *const _)
}

enum ExecuteResult {
    ExecuteSuccess,
    ExecuteTableFull,
    ExecuteDuplicateKey,
}

enum MetaCommandResult {
    MetaCommandSuccess,
    MetaCommandUnrecognizedCommand,
}

enum PrepareResult {
    PrepareSuccess,
    PrepareNegativeId,
    PrepareStringTooLong,
    PrepareSyntaxError,
    PrepareUnrecognizedStatement
}

enum StatementType {
    StatementInsert,
    StatementSelect,
    StatementNone,
}

enum NodeType {
    NodeInternal,
    NodeLeaf,
}
#[derive(Debug)]
struct InputBuffer {
    buffer : String,
}

struct Statement {
    kind: StatementType,
    row_to_insert: Row,
}

const COLUMN_USERNAME_SIZE:usize = 32;
const COLUMN_EMAIL_SIZE:usize = 255;

struct Row {
    id: usize,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

impl Row {
    fn new(id: usize, username: String, email: String) ->Self {
        Row {
            id,
            username: string_to_array!(username, COLUMN_USERNAME_SIZE),
            email: string_to_array!(email, COLUMN_EMAIL_SIZE),
        }
    }
}
const ID_SIZE:usize = std::mem::size_of::<usize>();
const USERNAME_SIZE:usize = 32;
const EMAIL_SIZE:usize = 255;

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
// const ROW_SIZE:usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const ROW_SIZE:usize = mem::size_of::<Row>();
const PAGE_SIZE:usize = 4096;
const TABLE_MAX_PAGES:usize = 100;

const NODE_TYPE_SIZE:usize = mem::size_of::<NodeType>();
const NODE_TYPE_OFFSET:usize = 0;
const IS_ROOT_SIZE:usize = mem::size_of::<bool>();
const IS_ROOT_OFFSET:usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE:usize = mem::size_of::<usize>();
const PARENT_POINTER_OFFSET:usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE:usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Leaf Node Header Layout
*/
const LEAF_NODE_NUM_CELLS_SIZE:usize = mem::size_of::<usize>();
const LEAF_NODE_NUM_CELLS_OFFSET:usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_NEXT_LEAF_SIZE: usize = std::mem::size_of::<usize>();
const LEAF_NODE_NEXT_LEAF_OFFSET: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;
/*
 * Leaf Node Body Layout
 */
const LEAF_NODE_KEY_SIZE:usize = mem::size_of::<usize>();
const LEAF_NODE_KEY_OFFSET:usize = 0;
const LEAF_NODE_VALUE_SIZE:usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET:usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE:usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS:usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS:usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
const LEAF_NODE_LEFT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/// Internal Node Header Layout
const INTERNAL_NODE_NUM_KEYS_SIZE: usize = std::mem::size_of::<usize>();
const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = std::mem::size_of::<usize>();
const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const INTERNAL_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/// Internal Node Body Layout
const INTERNAL_NODE_KEY_SIZE: usize = std::mem::size_of::<usize>();
const INTERNAL_NODE_CHILD_SIZE: usize = std::mem::size_of::<usize>();
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_SIZE;

#[derive(Clone, Copy)]
struct Page([u8; PAGE_SIZE]);

impl Page {
    fn new() -> Self {
        Self ([0u8; PAGE_SIZE])
    }

    unsafe fn row_mut_slot(&mut self, cell_num: usize) -> Row {
        fn read_end_idx(bytes: &[u8]) -> usize {
            for i in (0..bytes.len()).rev() {
                if bytes[i] != 0 {
                    return i;
                }
            }
            0
        }
        let cell = self.leaf_node_value(cell_num);

        let id = std::ptr::read(cell as *const usize);
        let username_bytes = std::ptr::read((cell as usize + USERNAME_OFFSET) as *const [u8; USERNAME_SIZE]);
        let email_bytes = std::ptr::read((cell as usize + EMAIL_OFFSET) as *const [u8; EMAIL_SIZE]);

        Row {
            id,
            username: username_bytes,
            email: email_bytes,
        }
    }

    fn is_full(&self) -> bool {
        unsafe {
            self.leaf_node_num_cells() >= LEAF_NODE_MAX_CELLS
        }
    }

    fn is_leaf_node(&self) -> bool {
        match (self.get_node_type()) {
            NodeInternal => {return false;}
            NodeLeaf => {return true;}
        }
    }

    fn index(&self, offset: usize) ->isize {
        let ptr = self.0.as_ptr();
        unsafe {
            (ptr as isize).checked_add(offset as isize).unwrap()
        }
    }

    unsafe fn leaf_node_mut_num_cells(&self) -> *mut usize {
        self.index(LEAF_NODE_NUM_CELLS_OFFSET) as *mut usize
    }

    pub fn get_leaf_node_next_leaf(&self) -> usize {
        unsafe {
            *(self.index(LEAF_NODE_NEXT_LEAF_OFFSET) as *const usize)
        }
    }
    fn set_leaf_node_num_cells(&mut self, num_cells: usize) {
        unsafe {
            *self.leaf_node_mut_num_cells() = num_cells
        }
    }

    pub fn set_leaf_node_next_leaf(&self, next_leaf: usize) {
        unsafe {
            *(self.index(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut usize) = next_leaf;
        }
    }

    fn leaf_node_num_cells(&self) -> usize {
        unsafe {*self.leaf_node_mut_num_cells()}
    }

    fn leaf_node_cell(&self, cell_num: usize) -> *const u8 {
        (self.index(LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE)) as *const u8
    }

    fn leaf_node_key(&self, cell_num: usize) -> usize {
        unsafe { *(self.leaf_node_cell(cell_num) as *mut usize) }
    }

    fn set_leaf_node_key(&self, cell_num: usize, key : usize){
        unsafe { *(self.leaf_node_cell(cell_num) as *mut usize) = key }
    }

    fn leaf_node_value(&self, cell_num: usize) -> *mut u8 {
        self.index(LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE) as *mut u8
    }

    fn initialize_leaf_node(&mut self) {
        self.set_node_type(NodeLeaf);
        self.set_node_root(false);
        self.set_leaf_node_next_leaf(0);
        self.set_leaf_node_num_cells(0);
    }

    fn initialize_internal_node(&mut self) {
        self.set_node_type(NodeInternal);
        self.set_node_root(false);
        let ptr = self.index(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut usize;
        unsafe {
            *ptr = 0;
        }
    }

    fn get_node_type<'a>(&self) -> &'a NodeType {
        unsafe {
            let ptr = self.index(NODE_TYPE_OFFSET) as *const NodeType;
            return &*ptr;
        }
    }

    fn set_node_type(&mut self, node_type: NodeType) {
        unsafe {
            let ptr = self.index(NODE_TYPE_OFFSET) as *mut NodeType;
            return *ptr = node_type;
        }
    }

    pub fn is_node_root(&self) -> bool {
        unsafe { *(self.index(IS_ROOT_OFFSET) as *const bool) }
    }

    pub fn set_node_root(&mut self, is_root: bool) {
        unsafe {
            *(self.index(IS_ROOT_OFFSET) as *mut bool) = is_root;
        }
    }

    fn internal_node_right_child(&self) -> isize {
        self.index(INTERNAL_NODE_RIGHT_CHILD_OFFSET)
    }

    pub fn set_internal_node_right_child(&mut self, internal_node_right_child: usize) {
        unsafe {
            *(self.internal_node_right_child() as *mut usize) = internal_node_right_child;
        }
    }

    pub fn get_internal_node_right_child(&self) -> usize {
        unsafe {
            *(self.internal_node_right_child() as *mut usize)
        }
    }

    pub fn set_internal_node_num_keys(&mut self, num_keys: usize) {
        unsafe {
            *(self.index(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut usize) = num_keys;
        }
    }

    pub fn get_internal_node_num_keys(&self) -> usize {
        unsafe {
            *(self.index(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut usize)
        }
    }

    pub fn internal_node_cell(&self, cell_num: usize) -> isize {
        self.index(INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE)
    }

    fn set_internal_node_cell(&mut self, cell_num: usize, page_num: usize) {
        unsafe { *(self.internal_node_cell(cell_num) as *mut usize) = page_num }
    }

    fn get_internal_node_cell(&self, cell_num: usize) -> usize {
        unsafe { *(self.internal_node_cell(cell_num) as *const usize) }
    }

    pub fn set_internal_node_child(&mut self, child_num: usize, child_page_num: usize) {
        let num_keys = self.get_internal_node_num_keys();
        if child_num > num_keys {
            println!("Tried to access child_num {} > num_keys {}", child_num, num_keys);
            process::exit(0x0010);
        } else if child_num == num_keys {
            self.set_internal_node_right_child(child_page_num);
        } else {
            self.set_internal_node_cell(child_num, child_page_num);
        }
    }

    pub fn get_internal_node_child(&self, child_num: usize) -> usize {
        let num_keys = self.get_internal_node_num_keys();
        if child_num > num_keys {
            println!("Tried to access child_num {}", child_num);
            process::exit(-1);
        } else if child_num == num_keys {
            self.get_internal_node_right_child()
        } else {
            self.get_internal_node_cell(child_num)
        }
    }

    pub fn set_internal_node_key(&mut self, key_num: usize, key_val: usize) {
        unsafe {
            *((self.internal_node_cell(key_num) + INTERNAL_NODE_CHILD_SIZE as isize) as *mut usize) = key_val;
        }
    }

    fn get_internal_node_key(&self, cell_num: usize) -> usize {
        unsafe {
            *((self.internal_node_cell(cell_num) + INTERNAL_NODE_CHILD_SIZE as isize) as *const usize)
        }
    }

    pub fn get_node_max_key(&self) -> usize {
        match self.get_node_type() {
            NodeInternal => self.get_internal_node_key(self.get_internal_node_num_keys() - 1),
            NodeLeaf => self.leaf_node_key(self.leaf_node_num_cells() - 1)
        }
    }
}

struct Pager {
    file_descriptor: File,
    file_length : usize,
    num_pages : usize,
    pages : [Option<Page>; TABLE_MAX_PAGES],
}

impl Pager {
    fn pager_open(filename : &str) -> Pager {
        let path = Path::new(filename);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .unwrap();

        let file_lenth = file.metadata().unwrap().len() as usize;
        let num_pages = file_lenth / PAGE_SIZE;

        if file_lenth % PAGE_SIZE != 0 {
            println!("Db file is not a whole number of pages. Corrupt file.");
            exit(-1);
        }

        let mut pager = Pager {
            file_descriptor: file,
            file_length: file_lenth,
            num_pages: num_pages,
            pages: [None; TABLE_MAX_PAGES],
        };

        if pager.num_pages == 0 {
            unsafe {
                let root_node = pager.get_page(0);
                root_node.initialize_leaf_node();
                root_node.set_node_root(true);
            }
        }
        return pager;
    }

    fn get_page(&mut self, page_num : usize) -> &mut Page {
        if page_num >= TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
            exit(-1);
        }
        if self.pages[page_num].is_none() {
            let mut page = Page::new();
            // partial page at the end of the file
            let num_pages = (self.file_length + PAGE_SIZE - 1) / PAGE_SIZE;

            if page_num <= num_pages {
                let res = self.file_descriptor.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64));

                if res.is_err() {
                    println!("Error seeking file {:?}", res);
                    exit(-1);
                }

                let res = self.file_descriptor.read(&mut page.0);
                if res.is_err() {
                    println!("Error reading file {:?}", res);
                    exit(-1);
                }
            }

            self.pages[page_num] = Some(page);

            if page_num >= self.num_pages {
                self.num_pages = page_num + 1;
            }
        }

        self.pages[page_num].as_mut().unwrap()
    }

    fn get_page_view(&self, page_num: usize) -> Page {
        if page_num > TABLE_MAX_PAGES {
            panic!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
        }

        self.pages[page_num].unwrap()
    }

    fn pager_flush(&mut self, page_num: usize) {
        let page = self.pages[page_num];
        if page.is_none() {
            println!("Tried to flush null page");
            exit(-1);
        }

        let offset = self.file_descriptor.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64));
        if offset.is_err() {
            println!("Error seeking file {:?}", offset);
            exit(-1);
        }

        let bytes_written = self.file_descriptor.write(&page.unwrap().0);
        let flush = self.file_descriptor.flush();
        if bytes_written.is_err() || flush.is_err() {
            println!("Error writing: {:?}, {:?}", bytes_written, flush);
            exit(-1);
        }
    }

    fn get_unused_page_num(&self) -> usize {
        self.num_pages
    }

    fn get_leftmost_leaf_page_num(&mut self, page_num: usize) -> usize {
        let page = self.get_page(page_num);

        if page.is_leaf_node() {
            return page_num;
        }
        let child_page_num = page.get_internal_node_child(0);
        return self.get_leftmost_leaf_page_num(child_page_num);
    }
}

struct Table {
    pager: Pager,
    root_page_num : usize,
}

impl Table {
    fn db_open(filename : &str) -> Self {
        let pager = Pager::pager_open(filename);

        Self {
            pager: pager,
            root_page_num: 0,
        }
    }

    fn db_close(&mut self) {
        let mut pager = &self.pager;

        for i in 0..pager.num_pages {
            let page = self.pager.pages[i];
            if page.is_none() {
                continue;
            }
            self.pager.pager_flush(i);
        }
    }

    fn execute_insert(&mut self, statement : &mut Statement) -> ExecuteResult {
        let row = &statement.row_to_insert;

        let (page_num, cell_num) = Cursor::table_find(self, row.id);
        let page = self.pager.get_page(page_num);

        if cell_num < page.leaf_node_num_cells() {
            let key_at_index = page.leaf_node_key(cell_num);
            if key_at_index == row.id {
                return ExecuteDuplicateKey;
            }
        }
        let mut cursor = Cursor {
            table : self,
            page_num,
            cell_num,
            end_of_table: false
        };
        unsafe {
            cursor.leaf_node_insert(row.id as usize, row);
        }

        return ExecuteSuccess;
    }

    fn execute_select(&mut self) -> ExecuteResult {
        let mut cursor = Cursor::table_start(self);
        while !cursor.end_of_table {
            let row = cursor.value();
            print_row(row);
            cursor.advance();
        }

        return ExecuteSuccess;
    }

    fn internal_node_find(&mut self, page_num: usize, key : usize)  -> (usize, usize) {
        let num_keys = self.pager.get_page(page_num).get_internal_node_num_keys();
        // binary search
        let (mut min_cell, mut max_cell) = (0, num_keys - 1);
        while min_cell < max_cell {
            let cell_num = (max_cell - min_cell) / 2 + min_cell;
            let cell_key_value = self.pager.get_page(page_num).get_internal_node_key(cell_num);
            if cell_key_value >= key {
                max_cell = cell_num;
            } else {
                min_cell = cell_num + 1;
            }
        }
        if self.pager.get_page(page_num).get_internal_node_key(max_cell) >= key {
            let child_page_num = self.pager.get_page(page_num).get_internal_node_child(max_cell);
            return self.find_by_page_num(child_page_num, key);
        }
        let right_child_num = self.pager.get_page(page_num).get_internal_node_right_child();
        self.find_by_page_num(right_child_num, key)
    }

    fn leaf_node_find(&mut self, page_num: usize, key: usize) -> (usize, usize) {
        let page = self.pager.get_page(page_num);
        let num_cells = page.leaf_node_num_cells();

        let mut min_index = 0;
        let mut one_past_max_index = num_cells;

        while one_past_max_index != min_index {
            let index = (min_index + one_past_max_index) / 2;
            let key_at_index = page.leaf_node_key(index);
            if key == key_at_index {
                return (page_num, index);
            } else if key < key_at_index {
                one_past_max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        return (page_num, min_index);
    }

    fn find_by_page_num(&mut self, page_num: usize, key: usize) -> (usize, usize) {
        match self.pager.get_page(page_num).get_node_type() {
            NodeInternal => {
                self.internal_node_find(page_num, key)
            }
            NodeLeaf => {
                self.leaf_node_find(page_num, key)
            }
        }
    }

    fn print_tree(&mut self) {
        fn print_tree_node(pager: &mut Pager, page_num: usize, indentation_level: usize) {
            fn indent(level: usize) {
                (0..level).for_each(|i| print!(" "));
            }
            match pager.get_page(page_num).get_node_type() {
                NodeLeaf => {
                    let num_keys = pager.get_page(page_num).leaf_node_num_cells();
                    indent(indentation_level);
                    println!("- leaf (size {})", num_keys);
                    for i in 0..num_keys {
                        indent(indentation_level + 1);
                        println!("{}", pager.get_page(page_num).leaf_node_key(i));
                    }
                },
                NodeInternal => {
                    let num_keys = pager.get_page(page_num).get_internal_node_num_keys();
                    indent(indentation_level);
                    println!("- internal (size {})", num_keys);
                    for i in 0..num_keys {
                        let child = pager.get_page(page_num).get_internal_node_child(i);
                        print_tree_node(pager, child, indentation_level + 1);
                        indent(indentation_level + 1);
                        println!("- key {}", pager.get_page(page_num).get_internal_node_key(i));
                    }
                    let child = pager.get_page(page_num).get_internal_node_right_child();
                    print_tree_node(pager, child, indentation_level + 1);
                }
            }
        }

        print_tree_node(&mut self.pager, 0, 0);
    }
}

struct Cursor<'a> {
    table : &'a mut Table,
    page_num : usize,
    cell_num : usize,
    end_of_table : bool,
}

impl <'a> Cursor<'a> {
    fn table_start(table: &'a mut Table) -> Cursor {
        let root_page_num = table.root_page_num;
        let leaf_page_num = table.pager.get_leftmost_leaf_page_num(root_page_num);
        let root_node = table.pager.get_page(leaf_page_num);
        let num_cells = root_node.leaf_node_num_cells();
        Cursor {
            table,
            page_num : leaf_page_num,
            cell_num : 0,
            end_of_table: num_cells == 0,
        }
    }

    fn table_find(table: &'a mut Table, key : usize) -> (usize, usize) {
        let root_node = table.pager.get_page(table.root_page_num);
        match root_node.get_node_type() {
            NodeType::NodeInternal => {
                return table.internal_node_find(table.root_page_num, key);
            }
            NodeType::NodeLeaf => {
                return table.leaf_node_find(table.root_page_num, key);
            }
        }
    }

    pub fn get_page_view(&self) -> Page {
        self.table.pager.get_page_view(self.page_num)
    }

    fn value(&mut self) -> Row {
        let page = self.table.pager.get_page(self.page_num);
        let cell_num = self.cell_num;
        unsafe { page.row_mut_slot(cell_num) }
    }

    fn advance(&mut self) {
        let page = self.table.pager.get_page(self.page_num);
        self.cell_num += 1;
        unsafe {
            if self.cell_num >= page.leaf_node_num_cells() {
                let next_page_num = page.get_leaf_node_next_leaf();
                if next_page_num == 0 {
                    /* This was rightmost leaf */
                    self.end_of_table = true;
                } else {
                    self.page_num = next_page_num;
                    self.cell_num = 0;
                }
            }
        }
    }

    unsafe fn leaf_node_insert(&mut self, key: usize, value: &Row) {
        let cell_num = self.cell_num;
        let page = self.table.pager.get_page(self.page_num);
        let num_cells = page.leaf_node_num_cells();
        if num_cells >= LEAF_NODE_MAX_CELLS {
            self.leaf_node_split_and_insert(key, value);
            return;
        }
        if cell_num < num_cells {
            // shift cell from cell_num to num_cells to right to make room for new cell
            for i in (cell_num + 1..=num_cells).rev() {
                std::ptr::copy_nonoverlapping(page.leaf_node_cell(i - 1),
                                              page.leaf_node_cell(i) as *mut u8,
                                              LEAF_NODE_CELL_SIZE);
            }
        }
        page.set_leaf_node_num_cells(num_cells + 1);
        page.set_leaf_node_key(cell_num, key);

        let cell = page.leaf_node_value(cell_num);
        self.serialize_row(cell, value);
    }

    fn leaf_node_split_and_insert(& mut self, key:usize, value: &Row) {
        /*
         Create a new node and move half the cells over.
         Insert the new value in one of the two nodes.
         Update parent or create a new parent.
        */
        let new_page_num = self.table.pager.get_unused_page_num();
        self.table.pager.get_page(new_page_num).initialize_leaf_node();
        let old_next_page_num = self.table.pager.get_page(new_page_num).get_leaf_node_next_leaf();
        self.table.pager.get_page(new_page_num).set_leaf_node_next_leaf(old_next_page_num);
        /*
         All existing keys plus new key should be divided
         evenly between old (left) and new (right) nodes.
         Starting from the right, move each key to correct position.
        */
        for i in (0..=LEAF_NODE_MAX_CELLS).rev() {
            let destination_node;
            if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
                destination_node = self.table.pager.get_page(new_page_num);
            } else {
                destination_node = self.table.pager.get_page(self.page_num);
            }
            let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
            let destination = destination_node.leaf_node_cell(index_within_node);

            if (i == self.cell_num) {
                destination_node.set_leaf_node_key(index_within_node, key);
                let cell = destination_node.leaf_node_value(index_within_node);
                unsafe {self.serialize_row(cell, value);}
            } else if (i > self.cell_num) {
                let src = self.table.pager.get_page(self.page_num).leaf_node_cell(i - 1);
                unsafe {std::ptr::copy(src, destination as *mut u8, LEAF_NODE_CELL_SIZE)}
            } else {
                let src = self.table.pager.get_page(self.page_num).leaf_node_cell(i);
                unsafe {std::ptr::copy(src, destination as *mut u8, LEAF_NODE_CELL_SIZE)}
            }
            self.table.pager.get_page(new_page_num).set_leaf_node_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT);
            self.table.pager.get_page(self.page_num).set_leaf_node_next_leaf(new_page_num);
        }
        /* Update cell count on both leaf nodes */
        let mut is_node_root = self.table.pager.get_page(self.page_num).is_node_root();
        self.table.pager.get_page(self.page_num).set_leaf_node_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT);
        self.table.pager.get_page(new_page_num).set_leaf_node_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT);
        self.table.pager.get_page(self.page_num).set_leaf_node_next_leaf(new_page_num);
        if (is_node_root) {
            return self.create_new_node(new_page_num);
        } else {
            println!("Need to implement updating parent after split");
            exit(-1);
        }
    }

    fn create_new_node(&mut self, right_child_page_num: usize) {
        // create new root node
        let left_child_page_num = self.table.pager.get_unused_page_num();
        let mut node_max_key;
        {
            let old_node = self.table.pager.get_page(self.page_num);
            let old_node_ptr = old_node as *const Page;
            let left_child = self.table.pager.get_page(left_child_page_num);
            unsafe {
                std::ptr::copy(old_node_ptr as *const u8, left_child as *mut Page as *mut u8, PAGE_SIZE);
                left_child.set_node_root(false);
            }
            node_max_key = left_child.get_node_max_key();
        }

        let old_node = self.table.pager.get_page(self.page_num);
        old_node.initialize_internal_node();
        old_node.set_node_root(true);
        old_node.set_internal_node_num_keys(1);
        old_node.set_internal_node_child(0, left_child_page_num);
        old_node.set_internal_node_key(0, node_max_key);
        old_node.set_internal_node_right_child(right_child_page_num);
    }

    unsafe fn serialize_row(&self, cell: *mut u8, source: &Row) {
        std::ptr::write(cell as *mut usize, source.id as usize);

        std::ptr::write((cell as usize + USERNAME_OFFSET) as *mut [u8; USERNAME_SIZE], [0u8; USERNAME_SIZE]);
        std::ptr::copy(source.username.as_ptr(), (cell as usize + USERNAME_OFFSET) as *mut u8, source.username.len());

        std::ptr::write((cell as usize + EMAIL_OFFSET) as *mut [u8; EMAIL_SIZE], [0 as u8; EMAIL_SIZE]);
        std::ptr::copy(source.email.as_ptr(), (cell as usize + EMAIL_OFFSET) as *mut u8, source.email.len());
    }
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

fn print_row(row : Row) {
    let trim_elems: [char; 1] = ['\0'];
    let username = String::from_utf8(row.username.to_vec()).expect("Error");
    let username = username.trim_end_matches(&trim_elems);
    let email = String::from_utf8(row.email.to_vec()).expect("Error");
    let email = email.trim_end_matches(&trim_elems);

    println!(
        "{} {:?} {:?}",
        row.id,
        username,
        email
    );
}

unsafe fn print_leaf_node(page : &mut Page) {
    let num_cells = page.leaf_node_num_cells();
    println!("leaf (size {})", num_cells);
    for i in 0..num_cells {
        let key = page.leaf_node_key(i);
        println!("  - {} : {}", i, key)
    }
}

fn do_meta_command(input_buffer : &InputBuffer, table: &mut Table) -> MetaCommandResult {
    match input_buffer.buffer.as_str() {
        ".exit" => {
            table.db_close();
            exit(0);
        }

        ".btree" => unsafe {
            println!("Tree: ");
            table.print_tree();
            return MetaCommandSuccess;
        }
        _ => {
            return MetaCommandUnrecognizedCommand;
        }
    }
}

impl Statement {
    fn new() -> Self {
        Self {
            kind: StatementNone,
            row_to_insert : Row {
                id: 0,
                username: [0u8; COLUMN_USERNAME_SIZE],
                email: [0u8; COLUMN_EMAIL_SIZE],
            }
        }
    }

    fn prepare_insert(&mut self, input_buffer: &InputBuffer) -> PrepareResult {
        self.kind = StatementInsert;
        let row =
            sscanf::sscanf!(input_buffer.buffer, "insert {usize} {str} {str}");
        if row.is_ok() {
            let (id,username, email) = row.unwrap();

            if username.len() > COLUMN_USERNAME_SIZE || email.len() > COLUMN_EMAIL_SIZE {
                return PrepareStringTooLong;
            }
            self.row_to_insert.id = id;
            self.row_to_insert.username = string_to_array!(username, COLUMN_USERNAME_SIZE);
            self.row_to_insert.email = string_to_array!(email, COLUMN_EMAIL_SIZE);
        } else {
            return PrepareSyntaxError;
        }

        return PrepareSuccess;
    }

    fn prepare_statement(&mut self, input_buffer : &InputBuffer) -> PrepareResult{
        if input_buffer.buffer.starts_with("insert") {
            return self.prepare_insert(input_buffer);
        } else if input_buffer.buffer.starts_with("select") {
            self.kind = StatementSelect;
        } else {
            return PrepareUnrecognizedStatement;
        }

        return PrepareSuccess;
    }

    fn execute_statement(&mut self, table : &mut Table) -> ExecuteResult {
        match self.kind {
            StatementInsert => {
                return table.execute_insert(self);
            }
            StatementSelect => {
                return table.execute_select();
            }

            _ => {
                return ExecuteSuccess;
            }
        }
    }
}

fn main() {
    let mut args = env::args();
    assert!(args.len() > 1);
    let mut table = Table::db_open(&args.nth(1).unwrap());
    loop {
        print_prompt();
        let input_buffer = read_input();
        if input_buffer.buffer.starts_with(".") {
            match do_meta_command(&input_buffer, &mut table) {
                MetaCommandSuccess => {continue;}
                MetaCommandUnrecognizedCommand => {
                    println!("Unrecognized command {:?}", input_buffer.buffer);
                    continue;
                }
            }
        }
        let mut statement = Statement { kind: StatementType::StatementInsert, row_to_insert: Row {
            id: 0,
            username: [0u8; COLUMN_USERNAME_SIZE],
            email: [0u8; COLUMN_EMAIL_SIZE],
        } };

        match statement.prepare_statement(&input_buffer) {
            PrepareSuccess => {
            }
            PrepareUnrecognizedStatement => {
                println!("Unrecognized keyword at start of '{}'.", &input_buffer.buffer);
                continue;
            }
            PrepareSyntaxError => {
                println!("Syntax error. Could not parse statement.");
                continue;
            }
            PrepareNegativeId => {
                println!("ID must be positive.");
                continue;
            }
            PrepareStringTooLong=> {
                println!("String is too long.");
                continue;
            }
        }

        match statement.execute_statement(&mut table) {
            ExecuteSuccess => {
                println!("Executed.");
            }
            ExecuteTableFull => {
                println!("Error: Table full.");
            }
            ExecuteResult::ExecuteDuplicateKey => {
                println!("Error: Duplicate key.");
            }
        }

    }
}
