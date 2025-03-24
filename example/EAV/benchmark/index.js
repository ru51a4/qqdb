import mysql from "../../../src/engine.js"
import fs from "fs"

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
mysql.table['IBLOCK_ELEMENTS'].data = JSON.parse(fs.readFileSync('./el.json', 'utf8'));

mysql.table['IBLOCK_PROP_VALUE'].data = JSON.parse(fs.readFileSync('./prop_value.json', 'utf8'));


//get list
const startTime = performance.now()
let items = mysql.query(`
    SELECT * FROM iblock_elements el  
        JOIN iblock_properties ip on el.iblock_id = ip.iblock_id 
        JOIN iblock_prop_value pv on el.id = pv.el_id
        WHERE pv.prop_id = ip.id
`)
const endTime = performance.now()
console.log(`els - ${mysql.table['IBLOCK_ELEMENTS'].data.length}, prop-value - ${mysql.table['IBLOCK_PROP_VALUE'].data.length}: time - ${endTime - startTime} milliseconds`)
