#include <whitedb/dbapi.h> /* or #include <dbapi.h> on Windows */

int main(int argc, char **argv) {
  void *db;
  db = wg_attach_database("1000", 2000000);
  return 0;
}

