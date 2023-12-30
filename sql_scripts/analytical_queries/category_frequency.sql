SELECT 
    c.name AS Category,
    sc.name AS SubCategory,
    COUNT(*) AS PublicationCount
FROM 
    dwh.publication AS p
JOIN 
    dwh.publication_category AS pc ON p.id = pc.publication_id
JOIN 
    dwh.category AS c ON pc.category_id = c.id
LEFT JOIN 
    dwh.sub_category AS sc ON pc.subcategory_id = sc.id
WHERE 
    p.update_date BETWEEN DATE '2015-01-01' AND DATE '2023-12-31'
GROUP BY 
    c.name, sc.name
ORDER BY 
    PublicationCount DESC