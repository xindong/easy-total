
OBJECTS = ListenThread.o LogGate.o NetProcessor.o TaskThread.o Socket.o ServerConfig.o TimeTick.o

TARGET = LogGate

.PHONY : all

all: $(TARGET)
$(TARGET): $(OBJECTS) $(LIBS)
	$(CXX) -o $(TARGET) $(OBJECTS) $(CXXLFLAGS)

$(OBJECTS): %.o: %.cpp 
	$(CXX) $(CXXFLAGS) -c $< -o $@ 

SRCS = $(OBJECTS:%.o=%.cpp)
DEPS = $(OBJECTS:%.o=.%.d)

include $(DEPS)
$(DEPS): .%.d: %.cpp 
	@$(CXX) -MM $(CXXFLAGS) $< > $@.$$$$;sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; rm -f $@.$$$$

.PHONY: clean
clean:
	rm  -rf $(OBJECTS) $(TARGET)
