create table previsioni_comuni
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
	old smallint,
	constraint previsioni_comuni_pk
		primary key (autoid),
	constraint previsioni_comuni_comuni_istat_fk
		foreign key (idcomune) references comuni
			on update cascade on delete cascade
);

alter table previsioni_comuni owner to postgres;

create unique index previsioni_comuni_autoid_uindex
	on previsioni_comuni (autoid);

