-- Normalize "UK" country codes to ISO 3166-1 alpha-2 "GB"
UPDATE companies SET country = 'GB' WHERE country = 'UK';
