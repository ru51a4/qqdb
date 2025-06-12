```
node ./example/diary/index.js


cd ./example/EAV/benchmark && node ./index.js
```
![Снимок экрана 2025-03-27 в 19 41 16](https://github.com/user-attachments/assets/2d5a4742-c595-4caf-a690-8ff9b4acb6f4)
  
online demo -> https://qqdb.netlify.app/  

```
feature:  
SELECT: col alias  
FROM: subquery, alias  
WHERE : <, >, =, <>, AND, OR, NESTED (1 = 1 AND (1 = 1)), IN(ids, subquery), NOT IN  
JOIN, LEFT JOIN: subquery, hash join, nested exp in ON - like a where  
ORDER BY: DESC, ASC (multiple cols)  
GROUP BY: multiple cols + function(max, min, avg, string_agg) + HAVING (nested exp like a where + COUNT(*))  
LIMIT, OFFSET  
PERFOMANCE aka indexes: hash index(first exp(=) in where), bst index(first exp(<,>) in where).  
```
