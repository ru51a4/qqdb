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
        mysql.cache_subquery = {};
        mysql.cache_sort = {}
        let data = mysql._query(SimpleSqlParserJs.build(str)[0]);
        mysql.cache = {};
        mysql.cache_subquery = {};
        mysql.cache_sort = {}
        return data
    }
    static _query(tt, prev) {
        let cache_key = JSON.stringify([tt, prev]) // todo check use prev attr;
        if (mysql.cache_subquery[cache_key]) {
            return mysql.cache_subquery[cache_key];
        }
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
        let ffilter = (el, arr) => {
            {
                let deep = (arr) => {
                    let res = [];
                    for (let j = 0; j <= arr.length - 1; j++) {
                        let val = {};
                        let left = arr[j].left;
                        left = el[left] ?? arr[j].left;
                        let right = arr[j].right;

                        if (arr[j].arr && arr[j].type != 'IN') {
                            res.push(arr[j].type)
                            res.push(deep(arr[j].arr))
                        } else if (arr[j].type == "IN") {
                            let t = [];
                            if (arr[j].right.columns) {

                                let tt = mysql._query(arr[j].right, el);
                                let _right = [];
                                for (let l = 0; l <= tt.length - 1; l++) {
                                    _right.push(...Object.values(tt[l]))
                                }
                                t = _right
                            }
                            if (arr[j].right.args) {
                                t = arr[j].right.args;
                            }
                            if (!t.map((c) => String(c)).includes(String(left))) {
                                val = 0
                            } else {
                                val = 1
                            }
                            if (arr[j].ttype == "AND" || arr[j].ttype == "OR") {
                                res.push(arr[j].ttype)
                            }
                            res.push(val)

                        } else {
                            if (!operation[arr[j].type](left, el[right] ?? ((prev && prev[right]) ? prev[right] : right))) {
                                val = 0;
                            } else {
                                val = 1;
                            }
                            if (arr[j].ttype == "AND" || arr[j].ttype == "OR") {
                                res.push(arr[j].ttype)
                            }
                            res.push(val)

                        }
                    }

                    let expp = res
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
                return deep(arr)
            }
        }
        let join = (row, jj) => {
            for (let j = jj; j <= jj; j++) {
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
                let __left = _query.joins[j].exp[0].left.split(".");
                if (!row[__left[0] + '.' + __left[1]]) {
                    let t = JSON.parse(JSON.stringify(_query.joins[j].exp[0].left));
                    _query.joins[j].exp[0].left = _query.joins[j].exp[0].right;
                    _query.joins[j].exp[0].right = t;
                }
                let left = _query.joins[j].exp[0].left.split(".");

                let isLEFT_JOIN = _query.joins[j].type == "LEFT";
                let right = _query.joins[j].exp[0].right.split(".");
                let j_table_right = mysql.table[aliasTable[right[0]]];
                let iRight = j_table_right?.col?.indexOf(right[1])
                if (!mysql.cache[jt]?.[iRight]) {
                    if (!mysql.cache[jt]) {
                        mysql.cache[jt] = {};
                    }
                    mysql.cache[jt][iRight] = {};

                    mysql.table[jt].data.forEach((c, i) => {
                        if (!mysql.cache[jt][iRight][c[iRight]]) {
                            mysql.cache[jt][iRight][c[iRight]] = [];
                        }
                        mysql.cache[jt][iRight][c[iRight]].push(i);
                    });
                }
                let f = false;
                for (let jj = 0; jj <= mysql.cache[jt]?.[iRight]?.[row[left[0] + '.' + left[1]]]?.length - 1; jj++) {
                    //
                    let _jj = mysql.cache[jt][iRight][row[left[0] + '.' + left[1]]][jj];
                    if (operation['='](left[0], right[0]) || operation['='](row[left[0] + '.' + left[1]], j_table_right.data[_jj][iRight])) {
                        let currJoinRow = mysql.getObj(jt, _jj, ja, _query.columns);
                        let __row = JSON.parse(JSON.stringify(row));
                        mysql.mergeObj(__row, currJoinRow)
                        let expp = JSON.parse(JSON.stringify(_query.joins[j].exp));
                        for (let d = 0; d <= expp.length - 1; d++) {
                            if (expp[d + 1]) {
                                expp[d].next = expp[d + 1].ttype
                            }
                        }
                        expp[0].left = 1;
                        expp[0].right = 1;
                        expp[0].type = "="

                        if (ffilter(__row, expp)) {
                            f = true
                            if (_query.joins.length - 1 == j) {
                                rrow.push(__row);
                            } else if (_query.joins.length - 1 - j > 0) {
                                join(__row, j + 1)
                            }
                        }
                    }
                }
                if (!f && isLEFT_JOIN) {
                    let __row = JSON.parse(JSON.stringify(row));
                    let tt = {};
                    mysql.table[jt].col.forEach((c) => {
                        tt[`${ja.toUpperCase()}.${c.toUpperCase()}`] = null;
                    })
                    mysql.mergeObj(__row, tt)
                    if (_query.joins.length - 1 == j) {
                        rrow.push(__row);
                    } else if (_query.joins.length - 1 - j > 0) {
                        join(__row, j + 1)
                    }
                }

            }
        }
        //
        //MAIN
        //
        let a = performance.now();
        let loop = null;
        let _from = _query.fromSources[0].table;
        if (typeof _from === 'object') {
            let ja = _query.fromSources[0].alias
            mysql.table[ja] = {};
            let subquery = mysql._query(_query.fromSources[0].table);
            if (!subquery.length) {
                //return;
            }
            mysql.table[ja].col = Object.keys(subquery[0]).map(c => c.split(".")[1]);
            mysql.table[ja].data = subquery.map((c) => Object.values(c));
            aliasTable[ja] = ja;
            _from = ja;
        }
        if (!_query.whereClauses.find((c) => c?.next === 'OR') && (_query.whereClauses[0]?.type == ">" || _query.whereClauses[0]?.type == "<")) {
            loop = [];
            let ttype = _query.whereClauses[0]?.type;
            let val = Number(prev?.[_query.whereClauses[0].right] ?? _query.whereClauses[0].right);
            let arr = mysql.table[_from].data;
            let lt = _query.whereClauses[0]?.left.split(".");
            let coll = mysql.table[_from].col.indexOf(lt[1] ?? lt[0])
            if (!mysql.cache_sort[_from]) {
                mysql.cache_sort[_from] = {};
            }
            let deep = (arr) => {
                if (!arr.length) {
                    return
                }
                let _node = {};
                var middle = arr[Math.floor((arr.length - 1) / 2)];
                _node.val = middle.val;
                _node.i = middle.i
                _node.left = deep(arr.filter((c, i) => i < Math.floor((arr.length - 1) / 2)));
                _node.right = deep(arr.filter((c, i) => i > Math.floor((arr.length - 1) / 2)));
                return _node;
            }
            if (!mysql.cache_sort[_from][coll]) {
                let root = deep(arr.map((c, i) => { return { val: [...c], i: i } }).sort((a, b) => a[coll] - b[coll]));
                mysql.cache_sort[_from][coll] = { root: root };
            }
            let root = mysql.cache_sort[_from][coll].root;
            let dfs = (node) => {
                if (Number(node.val[coll]) == Number(val)) {
                    if (ttype == "<" && node.left) {
                        dfs(node.left)
                    }
                    if (ttype == ">" && node.right) {
                        dfs(node.right)
                    }
                }
                if (ttype == "<" && Number(node.val[coll]) > val) {
                    if (node.left) {
                        dfs(node.left)
                    }
                }
                if (ttype == "<" && Number(node.val[coll]) < val) {
                    loop.push(node.i)
                    if (node.left) {
                        dfs(node.left)
                    }
                    if (node.right) {
                        dfs(node.right)
                    }
                }
                //
                if (ttype == ">" && Number(node.val[coll]) < val) {
                    if (node.right) {
                        dfs(node.right)
                    }
                }
                if (ttype == ">" && Number(node.val[coll]) > val) {
                    loop.push(node.i)
                    if (node.right) {
                        dfs(node.right)
                    }
                    if (node.left) {
                        dfs(node.left)
                    }
                }
            }
            dfs(root)
            loop = loop.sort((a, b) => a - b)
        } else {
            loop = mysql.table[_from].data.length
        }
        loop = Array.isArray(loop) ? loop : Array.from({ length: loop }, (_, i) => i);
        for (let ki = 0; ki <= loop.length - 1; ki++) {
            let i = loop[ki]
            let row = mysql.getObj(_from, i, _query.fromSources[0].alias, _query.columns);
            //join
            rrow = [];
            if (_query.joins.length) {
                join(row, 0, i)
            }
            else {
                rrow.push(row);
            }
            rrow = rrow.filter((el) => ffilter(el, _query.whereClauses));
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
        let b = performance.now();

        //group by
        if (_query.groupByColumns.length) {
            let grrow = [];
            //
            let delimiter = '';
            let g_alias;
            let gtype;
            let sumCol;
            let maxCol;
            let string_agg_col;
            let arr_aggregate = [];
            _query.columns.forEach((c) => {
                g_alias = c.alias;
                if (c.col.fn === "MAX") {
                    maxCol = c.alias ? '_.' + c.alias : c.col.args[0]
                    arr_aggregate.push({ gtype: 'MAX', col: c.col.args[0], alias: c.alias })
                }
                if (c.col.fn === "MIN") {
                    maxCol = c.alias ? '_.' + c.alias : c.col.args[0]
                    arr_aggregate.push({ gtype: 'MIN', col: c.col.args[0], alias: c.alias })
                }
                if (c.col.fn === "SUM") {
                    gtype = 'SUM';
                    sumCol = c.alias ? '_.' + c.alias : c.col.args[0]
                    arr_aggregate.push({ gtype: 'SUM', col: c.col.args[0], alias: c.alias })
                }
                if (c.col.fn === "AVG") {
                    gtype = 'AVG';
                    sumCol = c.alias ? '_.' + c.alias : c.col.args[0]
                    arr_aggregate.push({ gtype: 'AVG', col: c.col.args[0], alias: c.alias })
                }
                if (c.col.fn === "STRING_AGG") {
                    gtype = 'STRING_AGG';
                    string_agg_col = c.alias ? '_.' + c.alias : c.col.args[0]
                    delimiter = ', ';
                    arr_aggregate.push({ gtype: 'STRING_AGG', col: c.col.args[0], delimiter: delimiter, alias: c.alias })
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
                let arr_res = [];
                let currg
                arr_aggregate.forEach((item) => {
                    if (item.gtype == 'MAX') {
                        let currg = res[i].sort((b, a) => a[item.col] - b[item.col])[0][item.col]
                        arr_res[item.alias] = currg;
                    } else if (item.gtype == 'MIN') {
                        let currg = res[i].sort((a, b) => a[item.col] - b[item.col])[0][item.col]
                        arr_res[item.alias] = currg;
                    } else if (item.gtype == "SUM") {
                        let currg = res[i].reduce((acc, a) => acc += a[item.col], 0)
                        arr_res[item.alias] = currg;
                    } else if (item.gtype == "AVG") {
                        let currg = res[i].reduce((acc, a) => acc += a[item.col], 0) / res[i].length
                        arr_res[item.alias] = currg;
                    }
                    else if (item.gtype == "STRING_AGG") {
                        let currg = res[i].map((a) => a[item.col]).join(delimiter)
                        arr_res[item.alias] = currg;
                    }
                });

                res[i] = res[i][res[i].length - 1]
                res[i]['_.COUNT'] = length;
                Object.keys(arr_res).forEach((k) => {
                    res[i][k] = arr_res[k]
                })
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

        //one col
        if (_query.columns[0].col != "*") {
            let __res = [];
            for (let i = 0; i <= res.length - 1; i++) {
                __res[i] = {};
                let _arr = [];
                _query.columns.forEach((c) => {
                    if (!c.fn) {
                        let col = c.alias ?? c.col;
                        if (c.alias) {
                            __res[i]['_.' + col] = res[i][col];
                            _arr.push(col);
                        }
                        else if (res[i][col]) {
                            __res[i][col] = res[i][col];
                            _arr.push(col);

                        }
                        if (res[i]['_.' + col]) {
                            __res[i]['_.' + col] = res[i]['_.' + col];
                            _arr.push('_.' + col);
                        }
                    }
                });
                _arr = Object.keys(res[i]).filter((c) => c.includes('_.') && !_arr.includes(c))
                _arr.forEach((c) => {
                    __res[i][c] = res[i][c]
                });
            }
            res = __res
        }


        mysql.cache_subquery[cache_key] = res;
        return res
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
                if (columns[j].col.fn) {
                    continue
                }
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
            obj[_col] = r.data[j]?.[i];
        }
        for (let j = 0; j <= columns.length - 1; j++) {
            if (columns[j].col.fn) {
                continue
            }

            if (columns[j].col != "*" && typeof columns[j].col == 'string' && !columns[j].col.includes('.')) {
                obj['_.' + uuidv4()] = columns[j].col;
            }
        }
        return obj;
    }
}