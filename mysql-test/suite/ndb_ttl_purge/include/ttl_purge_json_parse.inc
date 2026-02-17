# suite/ndb_ttl_purge/include/ttl_purge_json_parse.inc
#
# Parse a JSON file and extract a field value.
#
# Input:
#   $json_file  - Path to the JSON file
#   $json_field - Field name to extract (supports dot notation, e.g. "config.enabled")
#
# Output:
#   $json_value - The extracted value (string)
#
# Note: Perl blocks cannot access mysqltest variables directly.
# We write them to temp files first, then perl reads them.

--exec echo '$json_file' > $MYSQL_TMP_DIR/_json_input_file.txt
--exec echo '$json_field' > $MYSQL_TMP_DIR/_json_input_field.txt

--perl
use strict;
use warnings;
use JSON::PP;

my $tmp = $ENV{MYSQL_TMP_DIR};

sub read_line {
  open my $f, '<', $_[0] or return '';
  my $l = <$f>; chomp $l; close $f;
  return $l;
}

my $json_path = read_line("$tmp/_json_input_file.txt");
my $field = read_line("$tmp/_json_input_field.txt");

# Read JSON file
my $json_text = "";
if (open my $fh, '<', $json_path) {
  local $/;
  $json_text = <$fh>;
  close $fh;
}

my $value = "";
if ($json_text ne "") {
  eval {
    my $data = decode_json($json_text);
    # Support dot notation for nested fields
    my @parts = split /\./, $field;
    my $current = $data;
    for my $part (@parts) {
      if (ref($current) eq 'HASH' && exists $current->{$part}) {
        $current = $current->{$part};
      } elsif (ref($current) eq 'ARRAY' && $part =~ /^\d+$/) {
        $current = $current->[$part];
      } else {
        $current = undef;
        last;
      }
    }
    if (defined $current) {
      if (ref($current) eq 'ARRAY') {
        $value = scalar @$current;  # Return array length
      } elsif (ref($current) eq 'HASH') {
        $value = scalar keys %$current;  # Return hash size
      } elsif (JSON::PP::is_bool($current)) {
        $value = $current ? "true" : "false";
      } else {
        $value = $current;
      }
    }
  };
}

open my $out, '>', "$tmp/json_result.inc" or die "Cannot write: $!";
print $out "--let \$json_value = $value\n";
close $out;
EOF

--source $MYSQL_TMP_DIR/json_result.inc
