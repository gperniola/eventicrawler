create table meteo_comuni
(
	autoid serial not null,
	idcomune text,
	comune text,
	data timestamp,
	primavera smallint,
	estate smallint,
	autunno smallint,
	inverno smallint,
	sereno smallint,
	coperto smallint,
	poco_nuvoloso smallint,
	pioggia smallint,
	temporale smallint,
	nebbia smallint,
	neve smallint,
	temperatura numeric,
	velocita_vento numeric,
	dati_presenti  integer default 1,
	constraint meteo_comuni_pk
		primary key (autoid),
	constraint meteo_comuni_comuni_istat_fk
		foreign key (idcomune) references comuni
			on update cascade on delete cascade
);

alter table meteo_comuni owner to postgres;

create unique index meteo_comuni_autoid_uindex
	on meteo_comuni (autoid);

