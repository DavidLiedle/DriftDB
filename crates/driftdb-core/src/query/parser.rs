use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until, take_while1},
    character::complete::{alpha1, alphanumeric1, char, multispace0},
    combinator::{map, opt, recognize},
    multi::{many0, separated_list0},
    sequence::{delimited, pair, preceded, terminated, tuple},
    IResult,
};

use super::{AsOf, Query, WhereCondition};
use crate::errors::{DriftError, Result};

fn ws<'a, F, O>(f: F) -> impl FnMut(&'a str) -> IResult<&'a str, O>
where
    F: FnMut(&'a str) -> IResult<&'a str, O> + 'a,
{
    delimited(multispace0, f, multispace0)
}

fn identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_")))),
    ))(input)
}

fn json_value(input: &str) -> IResult<&str, serde_json::Value> {
    if let Ok((input, _)) = char::<&str, nom::error::Error<&str>>('{')(input) {
        let (input, content) = take_until("}")(input)?;
        let (input, _) = char('}')(input)?;
        let json_str = format!("{{{}}}", content);
        match serde_json::from_str(&json_str) {
            Ok(val) => Ok((input, val)),
            Err(_) => Ok((input, serde_json::Value::String(json_str))),
        }
    } else if let Ok((input, _)) = char::<&str, nom::error::Error<&str>>('"')(input) {
        let (input, content) = take_until("\"")(input)?;
        let (input, _) = char('"')(input)?;
        Ok((input, serde_json::Value::String(content.to_string())))
    } else {
        let (input, raw) = recognize(tuple((
            opt(char('-')),
            take_while1(|c: char| c.is_numeric() || c == '.'),
        )))(input)?;
        if let Ok(num) = raw.parse::<f64>() {
            Ok((input, serde_json::json!(num)))
        } else {
            Ok((input, serde_json::Value::String(raw.to_string())))
        }
    }
}

fn create_table(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("CREATE"))(input)?;
    let (input, _) = ws(tag_no_case("TABLE"))(input)?;
    let (input, name) = ws(identifier)(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, _) = ws(tag_no_case("pk"))(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, primary_key) = ws(identifier)(input)?;

    let (input, indexed_columns) = opt(preceded(
        tuple((ws(char(',')), ws(tag_no_case("INDEX")), ws(char('(')))),
        terminated(
            separated_list0(ws(char(',')), ws(identifier)),
            ws(char(')')),
        ),
    ))(input)?;

    let (input, _) = ws(char(')'))(input)?;

    Ok((
        input,
        Query::CreateTable {
            name: name.to_string(),
            primary_key: primary_key.to_string(),
            indexed_columns: indexed_columns
                .unwrap_or_default()
                .iter()
                .map(|s| s.to_string())
                .collect(),
        },
    ))
}

fn insert(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("INSERT"))(input)?;
    let (input, _) = ws(tag_no_case("INTO"))(input)?;
    let (input, table) = ws(identifier)(input)?;

    // Find the JSON object starting from '{'
    let trimmed = input.trim_start();
    if !trimmed.starts_with('{') {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    // Find the matching closing brace
    let mut depth = 0;
    let mut in_string = false;
    let mut escape = false;
    let mut end_idx = 0;

    for (idx, ch) in trimmed.char_indices() {
        if escape {
            escape = false;
            continue;
        }

        match ch {
            '\\' if in_string => escape = true,
            '"' if !in_string => in_string = true,
            '"' if in_string => in_string = false,
            '{' if !in_string => depth += 1,
            '}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    end_idx = idx + 1;
                    break;
                }
            }
            _ => {}
        }
    }

    if depth != 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    let json_str = &trimmed[..end_idx];
    let data = serde_json::from_str(json_str)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))?;

    let remaining = &trimmed[end_idx..];

    Ok((
        remaining,
        Query::Insert {
            table: table.to_string(),
            data,
        },
    ))
}

fn patch(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("PATCH"))(input)?;
    let (input, table) = ws(identifier)(input)?;
    let (input, _) = ws(tag_no_case("KEY"))(input)?;
    let (input, primary_key) = ws(json_value)(input)?;
    let (input, _) = ws(tag_no_case("SET"))(input)?;

    // Parse the JSON object for updates
    let trimmed = input.trim_start();
    if !trimmed.starts_with('{') {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    let mut depth = 0;
    let mut in_string = false;
    let mut escape = false;
    let mut end_idx = 0;

    for (idx, ch) in trimmed.char_indices() {
        if escape {
            escape = false;
            continue;
        }

        match ch {
            '\\' if in_string => escape = true,
            '"' if !in_string => in_string = true,
            '"' if in_string => in_string = false,
            '{' if !in_string => depth += 1,
            '}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    end_idx = idx + 1;
                    break;
                }
            }
            _ => {}
        }
    }

    if depth != 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    let json_str = &trimmed[..end_idx];
    let updates = serde_json::from_str(json_str)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))?;

    let remaining = &trimmed[end_idx..];

    Ok((
        remaining,
        Query::Patch {
            table: table.to_string(),
            primary_key,
            updates,
        },
    ))
}

fn soft_delete(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("SOFT"))(input)?;
    let (input, _) = ws(tag_no_case("DELETE"))(input)?;
    let (input, _) = ws(tag_no_case("FROM"))(input)?;
    let (input, table) = ws(identifier)(input)?;
    let (input, _) = ws(tag_no_case("KEY"))(input)?;
    let (input, primary_key) = ws(json_value)(input)?;

    Ok((
        input,
        Query::SoftDelete {
            table: table.to_string(),
            primary_key,
        },
    ))
}

fn where_condition(input: &str) -> IResult<&str, WhereCondition> {
    let (input, column) = ws(identifier)(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, value) = ws(json_value)(input)?;

    Ok((
        input,
        WhereCondition {
            column: column.to_string(),
            operator: "=".to_string(),
            value,
        },
    ))
}

fn as_of_clause(input: &str) -> IResult<&str, AsOf> {
    let (input, _) = ws(tag_no_case("AS"))(input)?;
    let (input, _) = ws(tag_no_case("OF"))(input)?;

    alt((
        map(
            preceded(tag("@seq:"), take_while1(|c: char| c.is_numeric())),
            |s: &str| AsOf::Sequence(s.parse().unwrap_or(0)),
        ),
        map(tag("@now"), |_| AsOf::Now),
        map(
            delimited(char('"'), take_until("\""), char('"')),
            |s: &str| {
                time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .map(AsOf::Timestamp)
                    .unwrap_or(AsOf::Now)
            },
        ),
    ))(input)
}

fn select(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("SELECT"))(input)?;
    let (input, _) = ws(char('*'))(input)?;
    let (input, _) = ws(tag_no_case("FROM"))(input)?;
    let (input, table) = ws(identifier)(input)?;

    let (input, conditions) = opt(preceded(
        ws(tag_no_case("WHERE")),
        separated_list0(ws(tag_no_case("AND")), where_condition),
    ))(input)?;

    let (input, as_of) = opt(as_of_clause)(input)?;

    let (input, limit) = opt(preceded(
        ws(tag_no_case("LIMIT")),
        map(take_while1(|c: char| c.is_numeric()), |s: &str| {
            s.parse().unwrap_or(100)
        }),
    ))(input)?;

    Ok((
        input,
        Query::Select {
            table: table.to_string(),
            conditions: conditions.unwrap_or_default(),
            as_of,
            limit,
        },
    ))
}

fn show_drift(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("SHOW"))(input)?;
    let (input, _) = ws(tag_no_case("DRIFT"))(input)?;
    let (input, table) = ws(identifier)(input)?;
    let (input, _) = ws(tag_no_case("KEY"))(input)?;
    let (input, primary_key) = ws(json_value)(input)?;

    Ok((
        input,
        Query::ShowDrift {
            table: table.to_string(),
            primary_key,
        },
    ))
}

fn snapshot(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("SNAPSHOT"))(input)?;
    let (input, table) = ws(identifier)(input)?;

    Ok((
        input,
        Query::Snapshot {
            table: table.to_string(),
        },
    ))
}

fn compact(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("COMPACT"))(input)?;
    let (input, table) = ws(identifier)(input)?;

    Ok((
        input,
        Query::Compact {
            table: table.to_string(),
        },
    ))
}

fn quoted_path(input: &str) -> IResult<&str, String> {
    let (input, _) = char('\'')(input)?;
    let (input, path) = take_until("'")(input)?;
    let (input, _) = char('\'')(input)?;
    Ok((input, path.to_string()))
}

fn backup_database(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("BACKUP"))(input)?;
    let (input, _) = ws(tag_no_case("DATABASE"))(input)?;
    let (input, _) = ws(tag_no_case("TO"))(input)?;
    let (input, destination) = ws(quoted_path)(input)?;

    // Optional WITH COMPRESSION
    let (input, compression) = opt(preceded(
        tuple((ws(tag_no_case("WITH")), ws(tag_no_case("COMPRESSION")))),
        opt(preceded(
            ws(char('=')),
            ws(alt((
                map(tag_no_case("ZSTD"), |_| "zstd".to_string()),
                map(tag_no_case("GZIP"), |_| "gzip".to_string()),
                map(tag_no_case("NONE"), |_| "none".to_string()),
            ))),
        )),
    ))(input)?;

    // Optional INCREMENTAL
    let (input, incremental) = opt(ws(tag_no_case("INCREMENTAL")))(input)?;

    Ok((
        input,
        Query::BackupDatabase {
            destination,
            compression: compression.flatten(),
            incremental: incremental.is_some(),
        },
    ))
}

fn backup_table(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("BACKUP"))(input)?;
    let (input, _) = ws(tag_no_case("TABLE"))(input)?;
    let (input, table) = ws(identifier)(input)?;
    let (input, _) = ws(tag_no_case("TO"))(input)?;
    let (input, destination) = ws(quoted_path)(input)?;

    // Optional WITH COMPRESSION
    let (input, compression) = opt(preceded(
        tuple((ws(tag_no_case("WITH")), ws(tag_no_case("COMPRESSION")))),
        opt(preceded(
            ws(char('=')),
            ws(alt((
                map(tag_no_case("ZSTD"), |_| "zstd".to_string()),
                map(tag_no_case("GZIP"), |_| "gzip".to_string()),
                map(tag_no_case("NONE"), |_| "none".to_string()),
            ))),
        )),
    ))(input)?;

    Ok((
        input,
        Query::BackupTable {
            table: table.to_string(),
            destination,
            compression: compression.flatten(),
        },
    ))
}

fn restore_database(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("RESTORE"))(input)?;
    let (input, _) = ws(tag_no_case("DATABASE"))(input)?;
    let (input, _) = ws(tag_no_case("FROM"))(input)?;
    let (input, source) = ws(quoted_path)(input)?;

    // Optional TO target
    let (input, target) = opt(preceded(ws(tag_no_case("TO")), ws(quoted_path)))(input)?;

    // Optional WITH VERIFY
    let (input, verify) = opt(tuple((ws(tag_no_case("WITH")), ws(tag_no_case("VERIFY")))))(input)?;

    Ok((
        input,
        Query::RestoreDatabase {
            source,
            target,
            verify: verify.is_some(),
        },
    ))
}

fn restore_table(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("RESTORE"))(input)?;
    let (input, _) = ws(tag_no_case("TABLE"))(input)?;
    let (input, table) = ws(identifier)(input)?;
    let (input, _) = ws(tag_no_case("FROM"))(input)?;
    let (input, source) = ws(quoted_path)(input)?;

    // Optional TO target
    let (input, target) = opt(preceded(ws(tag_no_case("TO")), ws(quoted_path)))(input)?;

    // Optional WITH VERIFY
    let (input, verify) = opt(tuple((ws(tag_no_case("WITH")), ws(tag_no_case("VERIFY")))))(input)?;

    Ok((
        input,
        Query::RestoreTable {
            table: table.to_string(),
            source,
            target,
            verify: verify.is_some(),
        },
    ))
}

fn show_backups(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("SHOW"))(input)?;
    let (input, _) = ws(tag_no_case("BACKUPS"))(input)?;

    // Optional FROM directory
    let (input, directory) = opt(preceded(ws(tag_no_case("FROM")), ws(quoted_path)))(input)?;

    Ok((input, Query::ShowBackups { directory }))
}

fn verify_backup(input: &str) -> IResult<&str, Query> {
    let (input, _) = ws(tag_no_case("VERIFY"))(input)?;
    let (input, _) = ws(tag_no_case("BACKUP"))(input)?;
    let (input, backup_path) = ws(quoted_path)(input)?;

    Ok((input, Query::VerifyBackup { backup_path }))
}

fn sql_query(input: &str) -> IResult<&str, Query> {
    alt((
        backup_database,
        backup_table,
        restore_database,
        restore_table,
        show_backups,
        verify_backup,
        create_table,
        insert,
        patch,
        soft_delete,
        select,
        show_drift,
        snapshot,
        compact,
    ))(input)
}

pub fn parse_sql(input: &str) -> Result<Query> {
    match sql_query(input.trim()) {
        Ok((remaining, query)) => {
            if remaining.trim().is_empty() {
                Ok(query)
            } else {
                Err(DriftError::Parse(format!(
                    "Unexpected content after query: '{}'",
                    remaining
                )))
            }
        }
        Err(e) => Err(DriftError::Parse(format!("Parse error: {}", e))),
    }
}
