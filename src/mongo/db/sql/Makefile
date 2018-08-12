TARGET=mongo_sql
ARLIB = lib$(TARGET).a

rwildcard=$(wildcard $1$2) $(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2))

SRC_FILES := $(call rwildcard,postgres/,*.c) $(wildcard parse/*.c)
OBJ_FILES := $(SRC_FILES:.c=.o)

CFLAGS = -I. -I./postgres/include -Wfatal-errors
LIBPATH = -L.

CC ?= cc

AR = ar rs
ECHO = echo
RM = rm -f

CLEANLIBS = $(ARLIB)
CLEANOBJS = $(OBJ_FILES)

all: build

build: $(ARLIB)

clean:
	-@ $(RM) $(CLEANLIBS) $(CLEANOBJS)

.c.o:
	@$(ECHO) compiling $(<)
	@$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ -c $<

$(ARLIB): $(OBJ_FILES) Makefile
	$(AR) $@ $(OBJ_FILES)

TESTS = test/parse
test: $(TESTS)
	test/parse

test/parse: test/parse.c $(ARLIB)
	$(CC) -I. -o $@ -g test/parse.c $(ARLIB)
