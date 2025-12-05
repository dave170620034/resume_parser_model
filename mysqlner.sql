CREATE DATABASE  candidate_info;

USE candidate_info;

drop table candidate_details;
CREATE TABLE IF NOT EXISTS candidate_details (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255),
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    skills TEXT,
    education TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE candidate_details 
ADD UNIQUE(file_name);

desc candidate_details;

SELECT * FROM candidate_details;

