create table previsioni_eventi
(
	autoid serial not null,
	link text,
	titolo text,
	dataevento timestamp,
	idevento integer not null,
	idprevisione integer not null,
	constraint previsioni_eventi_pk
		primary key (autoid),
	constraint previsioni_eventi_eventi_autoid_fk
		foreign key (idevento) references eventi
			on update cascade on delete cascade,
	constraint previsioni_eventi_previsioni_comuni_autoid_fk
		foreign key (idprevisione) references previsioni_comuni
			on update cascade on delete cascade
);

alter table previsioni_eventi owner to postgres;

