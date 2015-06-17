package Catmandu::Store::DBI::Handler;

use Catmandu::Sane;
use Moo::Role;
use namespace::clean;

requires 'create_table';
requires 'add_row';

1;

