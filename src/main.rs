extern crate chrono;
extern crate regex;
extern crate xml;

use chrono::offset::Utc;
use chrono::DateTime;
use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::time::SystemTime;
use std::{fs, io::Write};

fn main() {
    let sql_raw = load_files_sql();

    let str_find_insert: &str = "INSERT INTO";
    let str_find_update: &str = "UPDATE";
    let vec_match_insert: Vec<_> = sql_raw.match_indices(str_find_insert).collect();
    let vec_match_update: Vec<_> = sql_raw.match_indices(str_find_update).collect();

    let mut vec_match_sql: Vec<(usize, &str)> =
        [&vec_match_insert[..], &vec_match_update[..]].concat();

    vec_match_sql.sort();

    let mut vec_sqlparser: Vec<SQLParser> = Vec::new();

    let mut count_match_sql: usize = 0;
    let it = &mut vec_match_sql.clone().into_iter();

    loop {
        match it.next() {
            Some(_) => {
                let ini = match vec_match_sql.get(count_match_sql) {
                    Some(index) => index,
                    None => &(0, ""),
                };
                let end = match vec_match_sql.get(count_match_sql + 1) {
                    Some(index) => index,
                    None => &(0, ""),
                };

                let index_ini = ini.0;
                let index_end = end.0;
                let statement_type = ini.1;

                let str_sql: &str;

                if index_end > 0 {
                    str_sql = &sql_raw[(index_ini)..(index_end)];
                } else {
                    str_sql = &sql_raw[(index_ini)..];
                }

                let statement_sql: String = str_sql
                    .replace('\n', "")
                    .replace('\r', "")
                    .replace('\t', "");

                let str_statement_sql = Box::leak(statement_sql.into_boxed_str());

                if statement_type == "INSERT INTO" {
                    let sqlparser: SQLParser = parser_insert(str_statement_sql);
                    vec_sqlparser.push(sqlparser);
                } else if statement_type == "UPDATE" {
                    let sqlparser: SQLParser = parser_update(str_statement_sql);
                    vec_sqlparser.push(sqlparser);
                }

                count_match_sql += 1;
            }
            None => {
                break;
            }
        };
    }

    let liquibase: String = generate_liquibase_xml(vec_sqlparser);

    let mut ofile = File::create("liquibase.xml").expect("unable to create file: liquibase.xml");

    ofile
        .write_all(liquibase.as_bytes())
        .expect("unable to write: liquibase.xml");
}

fn load_files_sql() -> std::string::String {
    let mut sql_raw = String::new();
    for element in std::path::Path::new("sql").read_dir().unwrap() {
        let path = element.unwrap().path();
        if let Some(extension) = path.extension() {
            if extension == "sql" {
                sql_raw += match &fs::read_to_string(&path) {
                    Ok(context) => context,
                    Err(_) => {
                        let path_print = &path.as_path().display().to_string();
                        panic!("Cannot read file: ".to_owned() + path_print);
                    }
                }
            }
        }
    }

    sql_raw
}

fn parser_insert(str_statement_sql: &mut str) -> SQLParser {
    let re = match Regex::new(
        r"^(INSERT INTO)(\s*)([A-Za-z_0-9]*)(\s*)[\(](.*)[\)](\s*)(VALUES)(\s*)[\(](.*)[\)]",
    ) {
        Ok(regex) => regex,
        Err(e) => panic!("{:?}", e),
    };
    let caps = match re.captures(str_statement_sql) {
        Some(cap) => cap,
        None => panic!("sql syntax not supported: ".to_owned() + str_statement_sql),
    };

    let table = caps.get(3).map_or("", |m| m.as_str());
    let fields = caps.get(5).map_or("", |m| m.as_str());
    let values = caps.get(9).map_or("", |m| m.as_str());

    let values = values
        .replace("GETDATE()", "'GETDATE()'")
        .replace("NEWID()", "'NEWID()'")
        .replace("NULL", "'NULL'");

    let fields: Vec<&str> = fields.split(",").collect();

    let mut values: Vec<&str> = values.split("\'").filter(|s| (s.trim() != ",")).collect();
    if let Some(pos) = values.iter().position(|x| *x == "") {
        values.remove(pos);
    }
    if let Some(pos) = values.iter().rposition(|x| *x == "") {
        values.remove(pos);
    }

    let mut map_fields_values: HashMap<&str, String> = HashMap::new();

    let mut count_field: usize = 0;
    for &field in &fields {
        let field: String = field.replace('\"', "").replace('\'', "");
        let str_field_trim = Box::leak(field.into_boxed_str()).trim();

        let str_value_trim: &str = match values.get(count_field) {
            Some(value) => value.trim(),
            None => panic!("number of values does not match fields: ".to_owned() + str_field_trim),
        };

        let value: String = str_value_trim
            .trim_start_matches('\'')
            .trim_end_matches('\'')
            .replace("\'\'", "\'");

        map_fields_values.insert(str_field_trim, value);

        count_field += 1;
    }

    let mut vec_fields_values: Vec<_> = map_fields_values.into_iter().collect();
    vec_fields_values.sort_by(|x, y| x.0.cmp(&y.0));

    let sqlparser = SQLParser {
        table,
        statement_type: "insert",
        values: vec_fields_values,
        update_where: "None".to_string(),
    };

    sqlparser
}

fn parser_update(str_statement_sql: &mut str) -> SQLParser {
    let re = match Regex::new(r"^(UPDATE)(\s*)([A-Za-z_0-9]*)(\s*)(SET)(.*)(WHERE)(.*)") {
        Ok(regex) => regex,
        Err(e) => panic!("{:?}", e),
    };

    let caps = match re.captures(str_statement_sql) {
        Some(cap) => cap,
        None => panic!("sql syntax not supported: ".to_owned() + str_statement_sql),
    };

    let table = caps.get(3).map_or("", |m| m.as_str());
    let sets = caps.get(6).map_or("", |m| m.as_str());
    let update_where = caps.get(8).map_or("", |m| m.as_str());

    let vec_sets: Vec<&str> = sets.split(",").collect();

    let mut map_fields_values: HashMap<&str, String> = HashMap::new();

    for item_set in vec_sets {
        let vec_item_set: Vec<&str> = item_set.split("=").collect();

        let str_field: &str = match vec_item_set.get(0) {
            Some(value) => value,
            None => panic!("no fields found: ".to_owned() + item_set),
        };

        let str_value: &str = match vec_item_set.get(1) {
            Some(value) => value.trim(),
            None => panic!("no values found: ".to_owned() + item_set),
        };

        let field = str_field.replace('\"', "").replace('\'', "");
        let str_field_trim = Box::leak(field.into_boxed_str()).trim();

        let value = str_value
            .trim_start_matches('\'')
            .trim_end_matches('\'')
            .replace("\'\'", "\'");

        map_fields_values.insert(str_field_trim, value);
    }

    let mut vec_fields_values: Vec<_> = map_fields_values.into_iter().collect();
    vec_fields_values.sort_by(|x, y| x.0.cmp(&y.0));

    let update_where: String = update_where.trim().to_string();

    let sqlparser = SQLParser {
        table,
        statement_type: "update",
        values: vec_fields_values,
        update_where,
    };

    sqlparser
}

fn generate_liquibase_xml(vec_sqlparser: Vec<SQLParser>) -> String {
    let mut liquibase_xml = String::new();

    let system_time = SystemTime::now();
    let datetime: DateTime<Utc> = system_time.into();
    let curr_date = datetime.format("%Y%m%d").to_string();

    let mut count_changeset: usize = 0;

    for item_sql in vec_sqlparser {
        liquibase_xml += &("<changeSet author=\"users\" id=\"".to_owned()
            + &curr_date
            + "-"
            + &count_changeset.to_string()
            + "\">\r\n");

        liquibase_xml += &("\t<".to_owned()
            + item_sql.statement_type
            + " tableName=\""
            + item_sql.table
            + &"\"> \r\n");

        for item_value in item_sql.values {
            if &item_value.1 == "GETDATE()" || &item_value.1 == "NEWID()" {
                liquibase_xml += &("\t\t<column name=\"".to_owned()
                    + item_value.0
                    + "\" valueComputed=\""
                    + &xml::escape::escape_str_attribute(&item_value.1)
                    + "\"/>\r\n");
            } else {
                liquibase_xml += &("\t\t<column name=\"".to_owned()
                    + item_value.0
                    + "\" value=\""
                    + &xml::escape::escape_str_attribute(&item_value.1)
                    + "\"/>\r\n");
            }
        }

        if item_sql.statement_type == "update" {
            liquibase_xml += &("\t\t<where>".to_owned() + &item_sql.update_where + "</where>\r\n");
        }

        liquibase_xml += &("\t</".to_owned() + item_sql.statement_type + ">\r\n");

        liquibase_xml += "</changeSet>\r\n";

        count_changeset += 1;
    }

    liquibase_xml
}
#[derive(Debug)]
struct SQLParser<'a> {
    statement_type: &'a str,
    table: &'a str,
    values: Vec<(&'a str, String)>,
    update_where: String,
}
