%define name radosgw-agent
%define version 1.1
%define unmangled_version 1.1
%define unmangled_version 1.1
%define release 1

Summary: Synchronize users and data between radosgw clusters
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: MIT
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Josh Durgin <josh.durgin@inktank.com>
Requires: python-argparse
Requires: PyYAML
Requires: python-boto >= 2.2.2
Requires: python-boto < 3.0.0
Requires: python-requests
Url: https://github.com/ceph/radosgw-agent

%description
UNKNOWN

%prep
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT
install -m 0755 -D scripts/radosgw-agent $RPM_BUILD_ROOT%{_bindir}/radosgw-agent
install -m 0644 -D logrotate.conf $RPM_BUILD_ROOT%{_sysconfdir}/logrotate.d/radosgw-agent
install -m 0755 -D init-radosgw-agent $RPM_BUILD_ROOT%{_initrddir}/radosgw-agent
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/ceph/radosgw-agent
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/log/ceph/radosgw-agent
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/run/ceph/radosgw-agent

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%doc LICENSE
%dir %{_sysconfdir}/ceph/radosgw-agent
%dir %{_localstatedir}/log/ceph/radosgw-agent
%dir %{_localstatedir}/run/ceph/radosgw-agent
%config(noreplace) %{_sysconfdir}/logrotate.d/radosgw-agent
%{_bindir}/radosgw-agent
%{_initrddir}/radosgw-agent
%{python_sitelib}/radosgw_agent*/
