SELECT 
    c.name AS Category_Name,
    AVG(v_latest.latest_date - v_earliest.earliest_date) AS Avg_Days_Between_Versions
FROM 
    dwh.publication AS p
JOIN 
    dwh.publication_category AS pc ON p.id = pc.publication_id
JOIN 
    dwh.category AS c ON pc.category_id = c.id
JOIN 
    (SELECT 
        publication_id, 
        MIN(create_date) AS earliest_date
     FROM 
        dwh.version
     GROUP BY 
        publication_id) AS v_earliest ON p.id = v_earliest.publication_id
JOIN 
    (SELECT 
        publication_id, 
        MAX(create_date) AS latest_date
     FROM 
        dwh.version
     GROUP BY 
        publication_id) AS v_latest ON p.id = v_latest.publication_id
GROUP BY 
    c.name
