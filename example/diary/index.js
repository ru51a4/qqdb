import mysql from "../../src/engine.js"


mysql.table = {
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


//index
let sortedBlogs = mysql.query(`
        SELECT * FROM diary d 
        JOIN (SELECT p.diary_id, max(p.id) as pid FROM posts p JOIN users u ON p.user_id = u.id GROUP BY p.diary_id) pp on d.id = pp.diary_id
        JOIN users u on d.user_id = u.id
        ORDER BY pp.pid DESC
    `);
console.log("=================")
sortedBlogs.forEach((blog) => {
    console.log(`# ${blog['D.ID']} ${blog['D.NAME']} [by ${blog["U.LOGIN"]}]`)
    console.log(`>> ${blog['PP.PID']} ${blog['PP.MSG']} [by ${blog["PP.LOGIN"]}]`)

});
console.log("=================")
//posts diary_id = 1
let blog1 = mysql.query(`
        SELECT * FROM posts p
        JOIN diary d on p.diary_id = d.id  
        JOIN users u on p.user_id = u.id
        WHERE 1 IN (SELECT 1 FROM diary dd WHERE dd.id = d.id and dd.id = 1)
        ORDER BY p.id ASC
    `);
console.log("")
console.log(`======START:${blog1[0]["D.NAME"]}======`)
blog1.forEach((el) => {
    console.log(`#${el['P.ID']} ${el['P.MSG']} [by ${el['U.LOGIN']}]`)
});