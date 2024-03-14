CREATE TABLE dbo.diabetes
(
    PatientID INT NOT NULL PRIMARY KEY,
	Pregnancies INT NOT NULL,
	PlasmaGlucose INT NOT NULL,
	DiastolicBloodPressure INT NOT NULL,
	TricepsThickness INT NOT NULL,
	SerumInsulin INT NOT NULL,
	BMI FLOAT NOT NULL,
	DiabetesPedigree FLOAT NOT NULL,
	Age INT NOT NULL,
	Diabetic BINARY NOT NULL

);
