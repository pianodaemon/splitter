use strict;
use warnings;

use lib qw(/home/pianodaemon/perl5/lib/perl5 ./temp/share/perl/5.30.0/ ./temp/lib/perl5);

use File::stat;
use Fcntl qw(:flock :mode);
use Storable;
use Digest::MD5 qw(md5_hex);
use IPC::Shareable;
use Bloom::Filter;

# It stands for a simple boolean definition.
use constant {
    true  => 1,
    false => 0,
};

# It enables debugging verbosity
our $debug = true;

sub lookup_shm {

  my ($shm_mem_key, $shm_mem_size, $shm_mem_mode_perms) = @_; 
  my %shared_data;
  my $handle = undef;

  unless (eval {
      $handle = tie %shared_data, 'IPC::Shareable', $shm_mem_key, {
        create    => 0,
        exclusive => 0,
        size      => $shm_mem_size,
        mode      => $shm_mem_mode_perms,
      };
  }) {
    printf STDERR "Non-available share memory segment featuring key: $shm_mem_key\n"; 
    $debug and printf STDERR "%s", $@ || 'Unknown failure';
  }

  if (defined $handle) {
    print STDERR "Recovering share memory segment with key $shm_mem_key\n";
    return \%shared_data;
  }

  unless (eval {
      $handle = tie %shared_data, 'IPC::Shareable', $shm_mem_key, {
        create    => 1,
        exclusive => 0,
        size      => $shm_mem_size,
        mode      => $shm_mem_mode_perms,
      };
  }) {
    $debug and printf STDERR "%s", $@ || 'Unknown failure';
  }

  if (defined $handle) {
    print STDERR "Setting up share memory segment with key $shm_mem_key\n";
    my $bloom_filter = Bloom::Filter->new(
      capacity   => 1000,
      error_rate => 0.01,
    );

    $shared_data{bloom_filter} = Storable::freeze($bloom_filter);
    return \%shared_data;
  }

  die "Not possible to harness a share memory segment with key $shm_mem_key\n";
}


sub is_spotted {
  my ($shared_ref, $k) = @_;

  tied(%$shared_ref)->lock(LOCK_EX);
  my $bf = Storable::thaw($shared_ref->{bloom_filter});
  my $found = $bf->check($k);
  tied(%$shared_ref)->unlock;
  return $found;
}


sub record {
  my ($shared_ref, $k) = @_;

  tied(%$shared_ref)->lock(LOCK_EX);
  my $bf = Storable::thaw($shared_ref->{bloom_filter});
  $bf->add($k);
  $shared_ref->{bloom_filter} = Storable::freeze($bf);
  tied(%$shared_ref)->unlock;
  return;
}


sub create_register {
  my ($file_path, $fetch_handler) = @_;

  my $ref_content = &$fetch_handler();

  # Serialize the data to disk
  Storable::nstore($ref_content, $file_path);
  return;
}


sub retrieve_register {
  my ($file_path, $ttl_expected) = @_;

  my $file_stat = stat($file_path);
  unless ($file_stat) {
    die "Failed to retrieve cache register $file_path: $!\n";
  }

  my $time_difference = time - $file_stat->mtime;
  if ($time_difference > $ttl_expected) {
    unlink $file_path;
    die "Cache register has expired.\n";
  }

  print "Asking for content via cache\n";
  my $sref = retrieve($file_path);
  return $sref;
}


sub fetcher_http_get_method {
  my $remote_url = shift;

  use LWP::UserAgent;
  my $ua = LWP::UserAgent->new;

  print "Asking for remote content via http get\n";
  my $get_response = $ua->get($remote_url);

  if ($get_response->is_success) {
    return \$get_response->decoded_content;
  }

  die "Failed to retrieve data. Error: ", $get_response->status_line, "\n";
}

my $shared_memory_key = 12345;
my $mode_perms = 0600;

# Size of the shared memory segment for Bloom filter
# 64K
my $cache_size = 1<<16;

my $shared_ref = lookup_shm($shared_memory_key, $cache_size, $mode_perms);

my $src_url = "https://httpbin.org/get";
my $kcache = md5_hex($src_url);
my $kfpath = $kcache . ".cache";
my $do_registration = sub {
  create_register($kfpath, sub {
    return fetcher_http_get_method $src_url;
  });
  return;
};

if ( is_spotted $shared_ref, $kcache ) {

RETRIEVE_POINT:
  my $sref;
  unless (eval {
    $sref = retrieve_register $kfpath, 30;
  }) {
    $debug and printf STDERR "%s", $@ || 'Unknown failure';
    &$do_registration();
    goto RETRIEVE_POINT;
  }
  print $$sref;
} else {
  &$do_registration();
  record($shared_ref, $kcache);
  goto RETRIEVE_POINT;
}
