Code Signing Certificate

Login to GoDaddy
	Name: 39077268
	pwd: gd4Revolution

Select the cert (revo_code_signing_cert)

Click "Download" to download the cert into your browser (Firefox)

Follow instructions to "backup" the cert into a file
	http://help.godaddy.com/topic/693/article/4783

	When saving, give a ".pfx" extension (and it will automatically get at ".p12" extension too)
	The password on the backup is the same as the GoDaddy login above.
	Rename backup to ".pfx" (to match names expected by Advanced Installer)

In Advanced Installer
	Click "Product Information" | "Digital Signature"
	General
		check "Sign the package"
		Application: SignTool.exe
		TimeStamp URL: http://tsa.starfieldtech.com
			- from http://help.godaddy.com/article/5412
	Software Publisher Certificate
		(*) File from Disk:
	Certificate Password
		(*) Store encrypted password in project file
