class Query {
  columns = [];
  fromSources = [];
  joins = [];
  whereClauses = [];
  havingClauses = [];
  groupByColumns = [];
  sortColumns = [];
  limit = []
} class SimpleSqlParserJs {
  static build = (input, num) => {
    input = input.split("").map((c) => c.toUpperCase());
    input = `( ${input.join("")} )`.split("\n").join(" ").split(",").join(" ").trim()
      .split("(").join(" ( ")
      .split(")").join(" ) ")
      .split(" ")
      .filter(c => !!c).map((s) => s.toUpperCase())

    let lex = (str) => {
      let query = new Query();
      let isColumns = false;
      let isFromSources = false;
      let isJoin = false;
      let isWhere = false;
      let isGroup = false;
      let isOrder = false;
      let isLimit = false;
      let isHaving = false;
      let typeJoin = '';

      let typeJoins = ["INNER", "LEFT", "RIGHT", "FULL"];
      while (str.length) {
        let token = str.shift();
        if (token === 'SELECT') {
          isColumns = true;
          continue
        }
        if (token === 'FROM') {
          isColumns = false;
          isFromSources = true;
          continue
        }
        if (typeJoins.includes(token)) {
          if (str[0] === 'OUTER') {
            str.shift();
            typeJoin = "FULL OUTER";
          }
          str.shift();
          typeJoin = token
          isFromSources = false;
          isJoin = true;
          continue
        }
        if (token === 'JOIN') {
          typeJoin = null;
          isFromSources = false;
          isJoin = true;
          continue
        }
        if (token === 'WHERE') {
          isFromSources = false;
          isJoin = false;
          isWhere = true;
          isHaving = false;
          continue
        }
        if (token === 'HAVING') {
          isFromSources = false;
          isJoin = false;
          isWhere = false;
          isHaving = true;
          isGroup = false;
          continue
        }
        if (token === 'GROUP') {
          isFromSources = false;
          isJoin = false;
          isWhere = false;
          isGroup = true;
          str.shift();
          continue
        }
        if (token === 'ORDER') {
          isFromSources = false;
          isJoin = false;
          isWhere = false;
          isHaving = false;
          isGroup = false;
          isOrder = true;
          str.shift();
          continue
        }
        if (token === 'LIMIT') {
          isFromSources = false;
          isJoin = false;
          isWhere = false;
          isGroup = false;
          isOrder = false;
          isHaving = false;
          isLimit = true;
          continue
        }
        if (isColumns) {
          query.columns.push(token)
          continue;
        }
        if (isFromSources) {
          query.fromSources.push(token)
          continue;
        }
        if (isJoin) {
          query.joins.push({ type: typeJoin, token })
          continue;
        }

        if (isWhere) {
          query.whereClauses.push(token)
          continue;
        }
        if (isGroup) {
          query.groupByColumns.push(token)
        }
        if (isOrder) {
          query.sortColumns.push(token)
        }
        if (isLimit) {
          query.limit.push(token)
        }
        if (isHaving) {
          query.havingClauses.push(token)
        }

      }
      let t = [];

      for (let i = 0; i <= query.fromSources.length - 1; i = i + 2) {
        t.push({ "table": query.fromSources[i], 'alias': query.fromSources[i + 1] })
      }
      query.fromSources = t;

      t = [];
      for (let i = 0; i <= query.columns.length - 1; i++) {
        if (query.columns[i + 1] === 'AS') {

          t.push({ "col": query.columns[i], 'alias': query.columns[i + 2] })
          i++
          i++;
        } else {
          t.push({ "col": query.columns[i] })
        }
      }
      query.columns = t;

      let deep = (arr) => {
        let t = [];
        for (let i = 0; i <= arr.length - 1; i = i + 3) {
          let next = (arr[i + 3]);
          let _next = null
          if (next?.fn == 'AND' || next?.fn == 'OR') {
            let a = deep(next.args)
            next = { _val: a.t, t_fn: next.fn }
            if (next) {
              //shiiit
              t.push({ "next": next.t_fn, "left": arr[i], 'right': arr[i + 2], 'type': arr[i + 1] })
              t.push({ "next": next, "left": arr[i], 'right': arr[i + 2], 'type': "=" })
              i++
              continue
            }

            //todo
            if (!arr[i + 4]?.fn) {
              _next = arr[i + 4];
            }
          }

          if (arr[i + 1]?.fn === 'IN') {
            next = (arr[i + 2]);
            t.push({ "next": next, "left": arr[i], 'right': arr[i + 1], 'type': '' })
          }
          else if (arr[i] === 'NOT EXISTS' || arr[i] === 'EXISTS') {
            //todo
            next = (arr[i + 2]);
            t.push({ "next": next, "left": arr[i], 'right': arr[i + 1], 'type': '' })

          }
          else if (_next) {
            t.push({ "_next": _next, "next": next, "left": arr[i], 'right': arr[i + 2], 'type': arr[i + 1] })
            i++
          }
          else if (next) {
            t.push({ "next": next, "left": arr[i], 'right': arr[i + 2], 'type': arr[i + 1] })
            i++
          }
          else {
            t.push({ "left": arr[i], 'right': arr[i + 2], 'type': arr[i + 1] })
          }
          if (next) {

          }
        }
        return { t };
      }

      t = [];
      let alias = false;
      for (let i = 0; i <= query.joins.length - 1; i = i + 1) {
        if (query.joins[i]?.token?.fn == "OR" || query.joins[i]?.token?.fn == "AND") {
          t[t.length - 1].exp.push({ 'ttype': query.joins[i].token?.fn, '__val': deep(query.joins[i].token.args).t, 'right': 1, 'type': "=" })
        }
        else if (query.joins[i]?.token === 'ON' || query.joins[i]?.token === 'AND' || query.joins[i]?.token === 'OR') {

          t[t.length - 1].exp.push({ 'ttype': query.joins[i].token, 'left': query.joins[i + 1]?.token, 'right': query.joins[i + 3]?.token, 'type': query.joins[i + 2]?.token })
          i++;
          i++;
          i++;
        } else {
          if (query.joins[i + 1]?.token === 'ON' || query.joins[i + 1]?.token === 'AND' || query.joins[i + 1]?.token === 'OR') {
            t.push({ 'exp': [], 'type': query.joins[i]?.type, "table": query.joins[i]?.token, 'alias': null })
          } else {
            t.push({ 'exp': [], 'type': query.joins[i]?.type, "table": query.joins[i]?.token, 'alias': query.joins[i + 1]?.token })
            i++;
          }

        }
      }
      query.joins = t;
      t = [];

      let asd = deep(query.whereClauses);
      t.push(...asd.t)

      query.whereClauses = t;

      t = [];
      for (let i = 0; i <= query.havingClauses.length - 1; i = i + 3) {
        let next = (query.havingClauses[i + 3]);
        if (next) {
          t.push({ "next": next, "left": query.havingClauses[i], 'right': query.havingClauses[i + 2], 'type': query.havingClauses[i + 1] })
          i++
        }
        else {
          t.push({ "left": query.havingClauses[i], 'right': query.havingClauses[i + 2], 'type': query.havingClauses[i + 1] })
        }
      }
      query.havingClauses = t;

      t = [];
      for (let i = 0; i <= query.groupByColumns.length - 1; i++) {
        t.push({ "col": query.groupByColumns[i] })
      }
      query.groupByColumns = t;
      t = [];
      for (let i = 0; i <= query.sortColumns.length - 1; i = i + 2) {

        t.push({ "col": query.sortColumns[i], 'type': query.sortColumns[i + 1] })
        i++
      }
      query.sortColumns = t;
      t = [];
      for (let i = 0; i <= query.limit.length - 1; i++) {
        t.push({ 'type': 'limit', "col": query.limit[i] })
        if (query.limit[i + 1] === 'OFFSET') {
          t.push({ 'type': 'OFFSET', "col": query.limit[i + 2] })
        }
        break
      }
      query.limit = t;
      return query;
    };
    let lexfn = (arr, fn) => {
      return { fn, args: [...arr] }
    };

    let t = [[]];
    let nested = (str) => {
      let tt = [];
      while (str.length) {
        let token = str.shift();
        if (token === '(') {
          if (tt[tt.length - 2] === 'NOT') {
            let rr = tt.pop();
            tt[tt.length - 1] += ` ` + rr;
          }
          t[t.length - 1].push(...tt);
          tt = [];
          t.push([])
        }
        else if (token === ')') {
          t[t.length - 1].push(...tt);
          tt = [];
          //
          let c = t[t.length - 1];
          t.pop();
          t[t.length - 1].push(c);
        } else {
          tt.push(token)
        }
      }

    }
    nested(input);

    let calc = (c) => {
      let type = stack[stack.length - 1] ?? 'SELECT'

      if (c[0] === "SELECT") {
        c.splice(0, c.length, { item: lex(c), complete: true })
      } else {
        c.splice(0, c.length, { item: lexfn(c, type), complete: true })
      }
    }
    let prev = {};
    let stack = [];
    let deep = (arr, init = true) => {

      for (let i = 0; i <= arr.length - 1; i++) {
        if (Array.isArray(arr[i])) {
          if (arr[i].length === 1 && arr[i][0]?.complete) {
            arr[i] = arr[i][0].item;
          } else {
            prev = arr[i - 1];
            stack.push(prev)
            if (arr[i][0] !== 'SELECT') {
              arr.splice(i - 1, 1)
              i--
            }
            deep(arr[i], 0);
            stack.pop();
            i = i - 1;
          }
        }
      }
      if (!init) {
        calc(arr)
      }
    }
    deep(t[0]);
    t = t[0]
    return t
  }
}


function uuidv4() {
  return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, c =>
    (+c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> +c / 4).toString(16)
  );
}
class mysql {
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
          for (let j = 0; j <= arr.length - 1; j++) {
            let left = arr[j].left;
            left = el[left] ?? arr[j].left;
            let right = arr[j].right;
            if (arr[j].__val) {
              arr[j].val = deep(arr[j]?.__val);
            }
            else if (arr[j]?.next?._val) {
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
              if (!right.map((c) => String(c)).includes(String(left))) {
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
        return deep(arr)

      }
    }
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

    if (!_query.whereClauses.find((c) => c?.next === 'OR') && (_query.whereClauses[0]?.type == ">" || _query.whereClauses[0]?.type == "<")) {
      loop = [];
      let ttype = _query.whereClauses[0]?.type;
      let val = Number(prev?.[_query.whereClauses[0].right] ?? _query.whereClauses[0].right);
      let arr = mysql.table[_query.fromSources[0].table].data;
      let lt = _query.whereClauses[0]?.left.split(".");
      let coll = mysql.table[_query.fromSources[0].table].col.indexOf(lt[1] ?? lt[0])
      if (!mysql.cache_sort[_query.fromSources[0].table]) {
        mysql.cache_sort[_query.fromSources[0].table] = {};
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
      if (!mysql.cache_sort[_query.fromSources[0].table][coll]) {
        let root = deep(arr.map((c, i) => { return { val: [...c], i: i } }).sort((a, b) => a[coll] - b[coll]));
        mysql.cache_sort[_query.fromSources[0].table][coll] = { root: root };
      }
      let root = mysql.cache_sort[_query.fromSources[0].table][coll].root;
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
      loop = mysql.table[_query.fromSources[0].table].data.length
    }
    loop = Array.isArray(loop) ? loop : Array.from({ length: loop }, (_, i) => i);
    for (let ki = 0; ki <= loop.length - 1; ki++) {
      let i = loop[ki]
      let row = mysql.getObj(_query.fromSources[0].table, i, _query.fromSources[0].alias, _query.columns);
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
    console.log(b - a)
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
      if (columns[j].col != "*" && typeof columns[j].col == 'string' && !columns[j].col.includes('.')) {
        obj['_.' + uuidv4()] = columns[j].col;
      }
    }
    return obj;
  }
}