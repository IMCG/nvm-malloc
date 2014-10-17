CC := gcc
debug:   CFLAGS := -O0 -ggdb -fpic -Wall -I. -Iulib-svn/include
release: CFLAGS := -O3 -fpic -Wall -I. -Iulib-svn/include
LDFLAGS := -lpthread

SRCDIR := src
OBJDIR := objects
OBJECTS := util.o chunk.o object_table.o arena.o nvm_malloc.o
LIBNAME := libnvmmalloc.so

release: $(LIBNAME)

debug: $(LIBNAME)

$(LIBNAME): ulib-svn/lib/libulib.a $(addprefix $(OBJDIR)/, $(OBJECTS))
	$(CC) $(CFLAGS) -shared -o $@ $(LDFLAGS) $(addprefix $(OBJDIR)/, $(OBJECTS)) ulib-svn/lib/libulib.a

$(OBJDIR)/%.o: $(SRCDIR)/%.c $(SRCDIR)/*.h
	@mkdir -p $(OBJDIR)
	$(CC) $(CFLAGS) -c -o $@ $<

ulib-svn/lib/libulib.a:
	cd ulib-svn; make;

clean:
	@rm -f $(LIBNAME)
	@rm -rf $(OBJDIR)

.PHONY: test debug release
