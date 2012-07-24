htmls = $(wildcard *.html)
pdfs = $(htmls:.html=.pdf)
pdf: $(pdfs)

%.pdf: %.html
	/Applications/wkhtmltopdf.app/Contents/MacOS/wkhtmltopdf $< $@
