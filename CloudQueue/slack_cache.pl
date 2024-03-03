use strict;
use warnings;

use lib qw(/home/pianodaemon/perl5/lib/perl5 ./temp/share/perl/5.30.0/ ./temp/lib/perl5);

use Fcntl qw(:flock);
use Storable;
use IPC::Semaphore;
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

my $shared_memory_key = 12345;
my $semaphore_key = 67890;
my $mode_perms = 0600;

# Size of the shared memory segment for Bloom filter
# 64K
my $cache_size = 1<<16;




#my $sem00 = lookup_sem($semaphore_key);
#print $sem00;

my $shared_ref = lookup_shm($shared_memory_key, $cache_size, $mode_perms);

#my $bloom_filter = Storable::thaw($share_ref->{bloom_filter});

#record($shared_ref, 'example1');
#record($shared_ref, 'example2');
#record($shared_ref, 'magneto');

print "Found \n" if is_spotted($shared_ref, 'example1');
print "Found \n" if is_spotted($shared_ref, 'example2');
print "Found \n" if is_spotted($shared_ref, 'magneto');
