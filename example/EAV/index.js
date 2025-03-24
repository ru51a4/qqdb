import mysql from "../../src/engine.js"


mysql.table = {
    "IBLOCK": {
        col: ['ID', 'NAME', 'PARENT_ID'],
        data: [
            [1, 'Каталог', 0],
            [2, 'Обувь', 1],
            [3, 'Тапочки', 2],
        ],
    },
    "IBLOCK_ELEMENTS": {
        col: ['ID', 'NAME', 'IBLOCK_ID'],
        data: [
            [1, 'Домашние Тапочки Розовый Рай', 3],
            [2, 'Домашние Тапочки Любимый Спорт', 3],
            [3, 'test group by', 3],

        ],
    },
    "IBLOCK_PROPERTIES": {
        col: ['ID', 'IS_NUMBER', 'IS_MULTY', 'NAME', 'IBLOCK_ID'],
        data: [
            [1, 0, 0, 'Артикул', 3],
            [2, 0, 0, 'Материал', 3],
        ],
    },
    "IBLOCK_PROP_VALUE": {
        col: ['ID', 'VALUE', 'VALUE_NUMBER', 'PROP_ID', 'EL_ID'],
        data: [
            [1, '174-15-xx', 0, 1, 1],
            [2, '174-16-xx', 0, 1, 2],
            [3, 'резина/кожа', 0, 2, 1],
            [4, 'текстиль/полимер', 0, 2, 2],
            [5, '174-15-xx', 0, 1, 3],
            [6, '174-15-xx', 0, 2, 3],
        ],
    },
};

//get list
console.log("catalog:")
let items = mysql.query(`
    SELECT * FROM iblock_elements el  
        JOIN iblock_properties ip on el.iblock_id = ip.iblock_id 
        JOIN iblock_prop_value pv on el.id = pv.el_id
        WHERE pv.prop_id = ip.id
`)
let prev = [];
items.forEach((item, i) => {
    if (i != 0 && prev != item['EL.NAME']) {
        console.log(" ");
    }
    prev = item['EL.NAME'];
    console.log(`${item['EL.NAME']}: ${item['IP.NAME']} -> ${item['PV.VALUE']}`)
});

//filter
let filter = mysql.query(`
    SELECT * FROM iblock_elements el  
        JOIN iblock_properties ip on el.iblock_id = ip.iblock_id 
        WHERE 1 = 1
        AND 1 IN (SELECT 1 FROM iblock_prop_value f WHERE f.el_id = el.id AND f.prop_id = 1 AND f.value = '174-16-xx')
        AND 1 IN (SELECT 1 FROM iblock_prop_value f WHERE f.el_id = el.id AND f.prop_id = 2 AND f.value = 'текстиль/полимер')
        GROUP BY el.id
`)
console.log("")
console.log('filtred:')
filter.forEach((item, i) => {
    console.log(`#${item['EL.ID']} - "${item['EL.NAME']}`)
});

//get all attribute by iblock id
console.log("")
console.log("attr:")
let attr = mysql.query(`
    SELECT * FROM iblock_properties ip JOIN iblock_prop_value pv ON ip.id = pv.prop_id 
    WHERE ip.iblock_id IN (1,2,3)
    GROUP BY pv.value, ip.id
`)
attr.forEach((el) => {
    console.log(`✅ ${el['IP.NAME']} - "${el['PV.VALUE']}"`)
});