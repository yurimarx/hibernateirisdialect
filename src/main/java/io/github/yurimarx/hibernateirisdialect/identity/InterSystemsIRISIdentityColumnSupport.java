/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package io.github.yurimarx.hibernateirisdialect.identity;

import org.hibernate.MappingException;
import org.hibernate.dialect.identity.IdentityColumnSupportImpl;

/**
 * 
 * Implementation of ID auto generation function
 * 
 * @author Yuri Marx Pereira Gomes
 * @author Andrea Boriero
 */
public class InterSystemsIRISIdentityColumnSupport  extends IdentityColumnSupportImpl {

	public static final InterSystemsIRISIdentityColumnSupport INSTANCE = new InterSystemsIRISIdentityColumnSupport();

	@Override
	public boolean supportsIdentityColumns() {
		return true;
	}

	@Override
	public boolean hasDataTypeInIdentityColumn() {
		// Whether this dialect has an Identity clause added to the data type or a completely seperate identity
		// data type
		return true;
	}

	@Override
	public String getIdentityColumnString(int type) throws MappingException {
		// The keyword used to specify an identity column, if identity column key generation is supported.
		return "identity";
	}

	@Override
	public String getIdentitySelectString(String table, String column, int type) {
		return "SELECT LAST_IDENTITY()";
	}
}
