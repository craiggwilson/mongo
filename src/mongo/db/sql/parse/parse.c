#include "parse.h"
#include <parser/parser.h>

void sql_parse(const char* input)
{
    List *tree;
    tree = raw_parser(input);
}