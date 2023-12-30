--- Calculate the total number of publications for each year

SELECT 
    EXTRACT(YEAR FROM p.update_date) AS PublicationYear,
    COUNT(*) AS TotalPublications
FROM 
    dwh.publication AS p
GROUP BY 
    PublicationYear
ORDER BY 
    PublicationYear