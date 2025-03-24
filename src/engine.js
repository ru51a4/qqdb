import SimpleSqlParserJs from "./parser.js"

function uuidv4() {
    return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, c =>
        (+c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> +c / 4).toString(16)
    );
}


export default class mysql {
    static table = {};

    static query(str) {
        mysql.cache = {};
        let data = mysql._query(SimpleSqlParserJs.build(str)[0]);
        mysql.cache = {};
        return data
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
                    if (!subquery.length) {
                        return;
                    }
                    mysql.table[ja].col = Object.keys(subquery[0]).map(c => c.split(".")[1]);
                    mysql.table[ja].data = subquery.map((c) => Object.values(c));
                    aliasTable[ja] = ja;
                    jt = ja;
                } else {
                    aliasTable[ja] = jt;
                }
                let jjj = [];

                let left = _query.joins[j].exp[0].left.split(".");
                let right = _query.joins[j].exp[0].right.split(".");
                let j_table_right = mysql.table[aliasTable[right[0]]];
                let iRight = j_table_right.col.indexOf(right[1])
                if (!mysql.cache[jt]?.[iRight]) {
                    mysql.cache[jt] = {};
                    mysql.cache[jt][iRight] = {};

                    mysql.table[jt].data.forEach((c, i) => {
                        if (!mysql.cache[jt][iRight][c[iRight]]) {
                            mysql.cache[jt][iRight][c[iRight]] = [];
                        }
                        mysql.cache[jt][iRight][c[iRight]].push(i);
                    });
                }
                for (let jj = 0; jj <= mysql.cache[jt]?.[iRight]?.[row[left[0] + '.' + left[1]]]?.length - 1; jj++) {
                    //
                    let _jj = mysql.cache[jt][iRight][row[left[0] + '.' + left[1]]][jj];
                    if (operation['='](row[left[0] + '.' + left[1]], j_table_right.data[_jj][iRight])) {
                        let currJoinRow = mysql.getObj(jt, _jj, ja, _query.columns);
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

                let deep = (arr) => {
                    for (let j = 0; j <= arr.length - 1; j++) {
                        let left = arr[j].left;
                        left = el[left] ?? arr[j].left;
                        let right = arr[j].right;
                        if (arr[j]?.next?._val) {
                            arr[j].val = deep(arr[j]?.next?._val);
                        }
                        else if (right.fn == "IN" || arr[j].type == "IN") {
                            if (right.fn !== "IN") {
                                let t = mysql._query(right, el);
                                right = [];
                                for (let l = 0; l <= t.length - 1; l++) {
                                    right.push(...Object.values(t[l]))
                                }
                            } else {
                                right = right.args
                            }
                            if (!right.includes(String(left))) {
                                arr[j].val = 0
                            } else {
                                arr[j].val = 1
                            }
                        } else {
                            if (!operation[arr[j].type](left, el[right] ?? ((prev && prev[right]) ? prev[right] : right))) {
                                arr[j].val = 0;
                            } else {
                                arr[j].val = 1;
                            }
                        }

                    }
                    let expp = [];
                    for (let i = 0; i <= arr.length - 1; i++) {

                        expp.push(arr[i].val)
                        if (!arr[i]?.next?._val && arr[i].next) {
                            expp.push(arr[i].next)
                        }
                        if (!arr[i]?._next?.fn && arr[i]._next) {
                            expp.push(arr[i]._next)
                        }
                    }
                    for (let i = 0; i <= expp.length - 1; i++) {
                        if (expp[i] == 'AND') {
                            let t = expp[i - 1] && expp[i + 1]
                            expp.splice(i - 1, 3, t);
                            i = i - 1;
                        }
                        if (expp[i] == 'OR') {
                            let t = expp[i - 1] || expp[i + 1]
                            expp.splice(i - 1, 3, t);
                            i = i - 1;
                        }
                    }
                    return expp[0] ?? 1;
                }
                return deep(_query.whereClauses)

            });
            //
            res.push(...rrow);

            if (_query.limit?.[0]) {
                let limit = Number(_query.limit?.[0]?.col)
                let offset = Number(_query.limit?.[1]?.col ?? 0)
                if (res.length >= limit + offset) {
                    res = res.filter((c, i) => i >= offset && i <= limit - 1)
                    break;
                }
            }
        }
        //one col
        let COL = null;

        if (_query.columns[0].col.includes(".") && _query.columns[0].col != "*" && _query.columns.length == 1) {
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
            for (let j = 0; j <= res.length - 1; j++) {
                let key = _query.groupByColumns.reduce((acc, a) => {
                    return acc + a.col + '-' + "(" + res[j][a.col] + ")"
                }, '')
                if (!grrow[key]) {
                    grrow[key] = [];
                }
                grrow[key].push(res[j]);
            }
            res = [...Object.values(grrow).filter((c) => c)];
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
        for (let j = 0; j <= columns.length - 1; j++) {
            if (typeof columns[j].col == 'string' && !columns[j].col.includes('.')) {
                obj['_.' + uuidv4()] = columns[j].col;
            }
        }
        return obj;
    }
}