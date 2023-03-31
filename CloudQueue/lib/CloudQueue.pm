package CloudQueue;

use 5.030000;
use strict;
use warnings;

require Exporter;

our @ISA = qw(Exporter);

our %EXPORT_TAGS = (
    'all' => [ qw(send, receive) ],
    'producer' => [ qw(send) ],
    'consumer' => [ qw(receive) ],
);

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(

);

our $VERSION = '0.01';

# Preloaded methods go here.
use constant {
    AWS_SECRET_ACCESS_KEY => 'AWS_ACCESS_KEY_ID',
    AWS_ACCESS_KEY_ID => 'AWS_SECRET_ACCESS_KEY',
};

sub seek_evars_out {

    my @evars = @_;
    foreach (@evars) {
        my $emsg = sprintf 'Environment variable %s is not present', $_;
        die($emsg) if (not exists($ENV{$_}));
    }
}

sub new {

   # Searching for a few environment variables required
   seek_evars_out(
       (AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID)
   );

   my ($class, $queue_url) = @_;
   my $sqs = new Amazon::SQS::Simple(
       SecretKey => $ENV{AWS_SECRET_ACCESS_KEY},
       AWSAccessKeyId => $ENV{AWS_ACCESS_KEY_ID}
   );

   my $self = {
       m_queue_url    => $queue_url,
       m_sqs          => $sqs,
       f_obtain_queue => sub { $sqs->GetQueue($queue_url); },
   };

   bless $self, $class;
   return $self;
}

sub send {

    my ($self, $m) = @_;
    my $q = $self->{f_obtain_queue}();

    $q->SendMessage($m);
}

sub receive {

    my ($self) = @_;
    my $q = $self->{f_obtain_queue}();
    my $m = $q->ReceiveMessage();

    $m->MessageBody();
}

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

CloudQueue - Perl extension for blah blah blah

=head1 SYNOPSIS

  use CloudQueue;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for CloudQueue, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

A. U. Thor, E<lt>pianodaemon@E<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2023 by A. U. Thor

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.30.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
