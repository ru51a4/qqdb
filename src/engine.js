import SimpleSqlParserJs from "./parser.js"

export default class mysql {
    static table = {
        "POSTS": {
            col: ['ID', 'MSG', 'USER_ID', 'DIARY_ID'],
            data: [
                [1, "Мой блог о php", 1, 1],
                [2, "Как дела?", 2, 1],
                [3, "Мой уютный блог обо всем на свете", 2, 2],
                [4, "Сегодня хорошая погода", 2, 2],
            ],
        },
        "USERS": {
            col: ["ID", "LOGIN", "STATUS"],
            data: [
                [1, "admin", 1],
                [2, "user", 3],
            ]
        },
        "DIARY": {
            col: ["ID", "NAME", "USER_ID"],
            data: [
                [1, "php и рефлексия", 2],
                [2, "уютный бложик", 1],
            ]
        }
    };

    static query(str) {
        return mysql._query(SimpleSqlParserJs.build(str)[0]);
    }
    static _query(tt, prev) {
        let operation = [];
        operation["<"] = (a, b) => {
            return a < b;
        }
        operation[">"] = (a, b) => {
            return a > b;
        }
        operation["="] = (a, b) => {
            if (typeof a == 'string') {
                return a.split("'").join("").toUpperCase() == b.split("'").join("").toUpperCase();
            }
            return a == b;
        }
        operation["<>"] = (a, b) => {
            return a != b;
        }
        let _query = tt;
        let res = [];
        let aliasTable = [];
        aliasTable[_query.fromSources[0].alias] = _query.fromSources[0].table;
        let rrow = [];

        let join = (row, jj) => {
            for (let j = jj; j <= jj; j++) {
                let jf = true;
                let jt = _query.joins[j].table
                let ja = _query.joins[j].alias;
                if (typeof _query.joins[j].table === "object") {
                    mysql.table[ja] = {};
                    let subquery = mysql._query(_query.joins[j].table, row);
                    mysql.table[ja].col = Object.keys(subquery[0]).map(c => c.split(".")[1]);
                    mysql.table[ja].data = subquery.map((c) => Object.values(c));
                    aliasTable[ja] = ja;
                    jt = ja;
                } else {
                    aliasTable[ja] = jt;
                }
                let jjj = [];
                for (let jj = 0; jj <= mysql.table[jt].data.length - 1; jj++) {
                    //
                    let left = _query.joins[j].exp[0].left.split(".");
                    let right = _query.joins[j].exp[0].right.split(".");
                    let j_table_right = mysql.table[aliasTable[right[0]]];
                    let iRight = j_table_right.col.indexOf(right[1])
                    if (operation['='](row[left[0] + '.' + left[1]], j_table_right.data[jj][iRight])) {
                        let currJoinRow = mysql.getObj(jt, jj, ja, _query.columns);
                        let __row = JSON.parse(JSON.stringify(row));
                        mysql.mergeObj(__row, currJoinRow)
                        if (_query.joins.length - 1 == j) {
                            rrow.push(__row);
                            rrow.alias = ja
                        } else if (_query.joins.length - 1 - j > 0) {
                            join(__row, j + 1)
                        }
                    }
                }
            }
        }

        for (let i = 0; i <= mysql.table[_query.fromSources[0].table].data.length - 1; i++) {
            let row = mysql.getObj(_query.fromSources[0].table, i, _query.fromSources[0].alias, _query.columns);
            //join
            rrow = [];
            if (_query.joins.length) {
                join(row, 0, i)
            }
            else {
                rrow.push(row);
            }
            rrow = rrow.filter((el) => {
                for (let j = 0; j <= _query.whereClauses.length - 1; j++) {

                    let left = _query.whereClauses[j].left;
                    left = el[left];
                    let right = _query.whereClauses[j].right;

                    if (right.fn == "IN" || _query.whereClauses[j].type == "IN") {
                        if (right.fn !== "IN") {
                            right = [...mysql._query(right, el).map((c) => String(Object.values(c)[0]))];
                        } else {
                            right = right.args
                        }
                        if (!right.includes(String(left))) {
                            return 0;
                        }
                    } else {
                        if (!operation[_query.whereClauses[j].type](left, (prev && prev[right]) ? prev[right] : right)) {
                            return 0;
                        }
                    }

                }
                return 1;
            });
            //
            res.push(...rrow);
        }
        //one col
        let COL = null;

        if (_query.columns[0].col != "*" && _query.columns.length == 1) {
            let __res = [];
            for (let i = 0; i <= res.length - 1; i++) {
                __res[i] = {};
                __res[i][_query.columns[0].col] = res[i][_query.columns[0].col];
            }
            res = __res
        }


        //group by
        if (_query.groupByColumns.length) {
            let grrow = [];
            //
            let selects = [];
            let maxCol = null;
            let countCol = null;
            _query.columns.forEach((c) => {
                if (c.col.fn === "MAX") {
                    maxCol = c.alias ? '_.' + c.alias : c.col.args[0]
                }
                if (c.col.fn === "COUNT(*)") {
                    countCol = true;
                }
            });
            //
            for (let i = 0; i <= _query.groupByColumns.length - 1; i++) {
                for (let j = 0; j <= res.length - 1; j++) {
                    if (!grrow[res[j][_query.groupByColumns[i].col]]) {
                        grrow[res[j][_query.groupByColumns[i].col]] = [];
                    }
                    grrow[res[j][_query.groupByColumns[i].col]].push(res[j]);
                }
            }
            res = [...grrow.filter((c) => c)];

            for (let i = 0; i <= res.length - 1; i++) {
                let length = res[i].length;
                if (maxCol) {
                    res[i] = res[i].sort((b, a) => a[maxCol] - b[maxCol])[0]
                    res[i]['_.COUNT'] = length;
                } else {
                    res[i] = res[i][res[i].length - 1]
                    res[i]['_.COUNT'] = length;
                }
            }
        }

        //ORDER BY
        if (_query.sortColumns.length) {
            if (_query.sortColumns[0].type == "DESC") {
                res = res.sort((b, a) => a[_query.sortColumns[0].col] - b[_query.sortColumns[0].col])
            } else {
                res = res.sort((a, b) => a[_query.sortColumns[0].col] - b[_query.sortColumns[0].col])
            }
        }

        return (res)
    }

    static mergeObj(obj, obj2) {
        Object.keys(obj2).forEach((key) => {
            obj[key] = obj2[key];
        })
    }

    static getObj(table, j, alias, columns) {
        let obj = {};
        let r = mysql.table[table];
        for (let i = 0; i <= r.col.length - 1; i++) {
            let _col = alias ? alias + '.' + r.col[i] : "" + r.col[i];
            for (let j = 0; j <= columns.length - 1; j++) {

                let getCol = (obj) => {
                    if (obj.col.args) {
                        return obj.col.args[0];
                    }
                    return obj.col
                };

                if (columns[j]?.alias && getCol(columns[j]) == alias + '.' + r.col[i]) {
                    _col = "_" + '.' + columns[j].alias;
                    break
                }
            }
            obj[_col] = r.data[j][i];
        }
        return obj;
    }
}
