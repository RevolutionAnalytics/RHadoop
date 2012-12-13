# see Makeconf for compiler settings
TARGET  = catwin

default:	$(TARGET) 

clean:
	rm -rf *~ *.o $(TARGET)

catwin: catwin.c $(OBJS) $(HEADERS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o catwin catwin.c $(OBJS) $(LIBS)

install-bin: $(TARGET)
	mkdir -p ../../$(BINDIR)
	cp $(TARGET) ../../$(BINDIR)
