class Query {
    columns = [];
    fromSources = [];
    joins = [];
    whereClauses = [];
    havingClauses = [];
    groupByColumns = [];
    sortColumns = [];
    limit = []
}
export default class SimpleSqlParserJs {
    static build = (input, num) => {
        /* токонезируем raw строку */
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
            /* конченный автомат(?) ложим токены в отдельные массив (where/join/etc) для дальнейшей обработки */
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

            t = [];
            let alias = false;
            /* рекурсивный спуск для парсинга условий в join/where */
            let deep = (arr) => {
                let t = [];
                for (let i = 0; i <= arr.length - 1; i++) {
                    if (arr[i].fn && arr[i].fn !== "IN" && arr[i].fn !== 'COUNT') {
                        let tt = deep(arr[i].args)
                        t.push({ "arr": tt, 'type': arr[i].fn })
                    }
                    if (arr[i] == '=' || arr[i] == '<>' || arr[i] == '<' || arr[i] == '>') {
                        t.push({ ttype: arr[i - 2] ?? '', "left": arr[i - 1], 'right': arr[i + 1], 'type': arr[i] })
                    }
                    if (arr[i] === "IN" || arr[i] === "NOT IN") {
                        t.push({ ttype: arr[i - 2] ?? '', "left": arr[i - 1], 'right': arr[i + 1], 'type': arr[i] })
                    }
                    if (arr[i]?.fn === "IN" || arr[i]?.fn === "NOT IN") {
                        t.push({ ttype: arr[i - 2] ?? '', "left": arr[i - 1], 'right': arr[i], 'type': arr[i] })
                    }
                }
                return t;
            }

            for (let i = 0; i <= query.joins.length - 1; i = i + 1) {
                if (query.joins[i]?.token?.fn == "OR" || query.joins[i]?.token?.fn == "AND") {
                    t[t.length - 1].exp.push(...deep(['AND', '1', '=', '1', query.joins[i]?.token?.fn, ...query.joins[i].token.args]))
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


            t.push(...deep(['1', '=', '1', 'AND', ...query.whereClauses]))
            query.whereClauses = t;


            t = [];
            t.push(...deep(['1', '=', '1', 'AND', ...query.havingClauses]))

            query.havingClauses = t;

            t = [];
            for (let i = 0; i <= query.groupByColumns.length - 1; i++) {
                t.push({ "col": query.groupByColumns[i] })
            }
            query.groupByColumns = t;
            t = [];
            for (let i = 0; i <= query.sortColumns.length - 1; i++) {
                t.push({ "col": query.sortColumns[i], 'type': query.sortColumns[i + 1] })
                i++;
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
        /* лексинг функций в SELECT, напр: MAX(id). так же тут лексяться выражения в where/join 1 OR (1 =2 OR (2 = 3)) */
        let lexfn = (arr, fn) => {
            return { fn, args: [...arr] }
        };

        /* подготовка для рекурсивный спуск для 
        подзапросов (смотрим на открывющиеся скобки и закрывающиеся - вкладывам токены массивов в токены массивов)*/
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
        //  Рекурсивный спуск(подзапросы), лексинг
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