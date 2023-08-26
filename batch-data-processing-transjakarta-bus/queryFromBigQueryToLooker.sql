WITH dim2 AS (
  SELECT r.id, r.kode_trayek, r.trayek, v.jenis,
  FROM `golden-union-392713.transjakarta.route_dim` r
  LEFT JOIN `golden-union-392713.transjakarta.vehicle_dim` v
    ON r.vehicle_fk=v.id
)

SELECT dim1.bulan, dim2.kode_trayek, dim2.trayek, dim2.jenis, f.jumlah_penumpang, 
FROM `golden-union-392713.transjakarta.fact_table` f

LEFT JOIN `golden-union-392713.transjakarta.date_dim` dim1
  ON f.date_fk=dim1.id

LEFT JOIN dim2
  ON f.route_fk=dim2.id
  
WHERE (
  dim2.trayek IS NOT NULL
) AND (
  f.route_fk IN (
    SELECT route_fk
    FROM `golden-union-392713.transjakarta.fact_table`
    GROUP BY route_fk
    HAVING COUNT(*) = 12
  )
);
