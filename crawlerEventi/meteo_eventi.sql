create table meteo_eventi
(
	autoid serial not null,
	link text,
	titolo text,
	dataevento timestamp,
	idevento integer not null,
	idmeteo integer not null,
	constraint meteo_eventi_pk
		primary key (autoid),
	constraint meteo_eventi_eventi_autoid_fk
		foreign key (idevento) references eventi
			on update cascade on delete cascade,
	constraint meteo_eventi_meteo_comuni_autoid_fk
		foreign key (idmeteo) references meteo_comuni
			on update cascade on delete cascade
);

alter table meteo_eventi owner to postgres;

